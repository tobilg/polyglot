//! Fluent SQL Builder API
//!
//! Provides a programmatic way to construct SQL [`Expression`] trees without parsing raw SQL
//! strings. The API mirrors Python sqlglot's builder functions (`select()`, `from_()`,
//! `condition()`, etc.) and is the primary entry point for constructing queries
//! programmatically in Rust.
//!
//! # Design
//!
//! The builder is organized around a few key concepts:
//!
//! - **Expression helpers** ([`col`], [`lit`], [`star`], [`null`], [`boolean`], [`func`],
//!   [`cast`], [`alias`], [`sql_expr`], [`condition`]) create leaf-level [`Expr`] values.
//! - **Query starters** ([`select`], [`from`], [`delete`], [`insert_into`], [`update`])
//!   return fluent builder structs ([`SelectBuilder`], [`DeleteBuilder`], etc.).
//! - **[`Expr`]** wraps an [`Expression`] and exposes operator methods (`.eq()`, `.gt()`,
//!   `.and()`, `.like()`, etc.) so conditions can be built without manual AST construction.
//! - **[`IntoExpr`]** and **[`IntoLiteral`]** allow ergonomic coercion of `&str`, `i64`,
//!   `f64`, and other primitives wherever an expression or literal is expected.
//!
//! # Examples
//!
//! ```
//! use polyglot_sql::builder::*;
//!
//! // SELECT id, name FROM users WHERE age > 18 ORDER BY name LIMIT 10
//! let expr = select(["id", "name"])
//!     .from("users")
//!     .where_(col("age").gt(lit(18)))
//!     .order_by(["name"])
//!     .limit(10)
//!     .build();
//! ```
//!
//! ```
//! use polyglot_sql::builder::*;
//!
//! // CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END
//! let expr = case()
//!     .when(col("x").gt(lit(0)), lit("positive"))
//!     .else_(lit("non-positive"))
//!     .build();
//! ```
//!
//! ```
//! use polyglot_sql::builder::*;
//!
//! // SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id LIMIT 5
//! let expr = union_all(
//!     select(["id"]).from("a"),
//!     select(["id"]).from("b"),
//! )
//! .order_by(["id"])
//! .limit(5)
//! .build();
//! ```

pub mod plan;

use crate::ast_mutation as engine;
use crate::expressions::*;
use crate::generator::{Generator, GeneratorConfig, NotInStyle};
use crate::parser::Parser;

/// Controls whether a repeated list or predicate clause appends to the existing
/// clause (`true`, the default) or replaces it (`false`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClauseOptions {
    pub append: bool,
}

impl Default for ClauseOptions {
    fn default() -> Self {
        Self { append: true }
    }
}

/// Options for a `LATERAL VIEW` clause.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LateralViewOptions {
    pub outer: bool,
}

/// Options for a `CREATE TABLE AS SELECT` statement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CtasOptions {
    pub replace: bool,
    pub temporary: bool,
}

fn generate_builder_sql(expression: &Expression) -> String {
    let mut generator = Generator::with_config(GeneratorConfig {
        not_in_style: NotInStyle::Infix,
        ..Default::default()
    });
    generator.generate(expression).unwrap_or_default()
}

fn is_safe_identifier_name(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }

    chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn builder_identifier(name: &str) -> Identifier {
    if name == "*" || is_safe_identifier_name(name) {
        Identifier::new(name)
    } else {
        Identifier::quoted(name)
    }
}

fn builder_table_ref(name: &str) -> TableRef {
    let parts: Vec<&str> = name.split('.').collect();

    match parts.len() {
        3 => {
            let mut t = TableRef::new(parts[2]);
            t.name = builder_identifier(parts[2]);
            t.schema = Some(builder_identifier(parts[1]));
            t.catalog = Some(builder_identifier(parts[0]));
            t
        }
        2 => {
            let mut t = TableRef::new(parts[1]);
            t.name = builder_identifier(parts[1]);
            t.schema = Some(builder_identifier(parts[0]));
            t
        }
        _ => {
            let first = parts.first().copied().unwrap_or("");
            let mut t = TableRef::new(first);
            t.name = builder_identifier(first);
            t
        }
    }
}

// ---------------------------------------------------------------------------
// Expression helpers
// ---------------------------------------------------------------------------

/// Create a column reference expression.
///
/// If `name` contains a dot, it is split on the **last** `.` to produce a table-qualified
/// column (e.g. `"u.id"` becomes `u.id`). Unqualified names produce a bare column
/// reference.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::col;
///
/// // Unqualified column
/// let c = col("name");
/// assert_eq!(c.to_sql(), "name");
///
/// // Table-qualified column
/// let c = col("users.name");
/// assert_eq!(c.to_sql(), "users.name");
/// ```
pub fn col(name: &str) -> Expr {
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() >= 3 && parts.iter().all(|part| !part.is_empty()) {
        let mut expr = Expression::boxed_column(Column {
            name: builder_identifier(parts[1]),
            table: Some(builder_identifier(parts[0])),
            join_mark: false,
            trailing_comments: Vec::new(),
            span: None,
            inferred_type: None,
        });

        for field in &parts[2..] {
            expr = Expression::Dot(Box::new(DotAccess {
                this: expr,
                field: builder_identifier(field),
                inferred_type: None,
            }));
        }

        return Expr(expr);
    }

    if let Some((table, column)) = name.rsplit_once('.') {
        Expr(Expression::boxed_column(Column {
            name: builder_identifier(column),
            table: Some(builder_identifier(table)),
            join_mark: false,
            trailing_comments: Vec::new(),
            span: None,
            inferred_type: None,
        }))
    } else {
        Expr(Expression::boxed_column(Column {
            name: builder_identifier(name),
            table: None,
            join_mark: false,
            trailing_comments: Vec::new(),
            span: None,
            inferred_type: None,
        }))
    }
}

/// Create a literal expression from any type implementing [`IntoLiteral`].
///
/// Supported types include `&str` / `String` (string literal), `i32` / `i64` / `usize` /
/// `f64` (numeric literal), and `bool` (boolean literal).
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::lit;
///
/// let s = lit("hello");   // 'hello'
/// let n = lit(42);        // 42
/// let f = lit(3.14);      // 3.14
/// let b = lit(true);      // TRUE
/// ```
pub fn lit<V: IntoLiteral>(value: V) -> Expr {
    value.into_literal()
}

/// Create a star (`*`) expression, typically used in `SELECT *`.
pub fn star() -> Expr {
    Expr(Expression::star())
}

/// Create a SQL `NULL` literal expression.
pub fn null() -> Expr {
    Expr(Expression::Null(Null))
}

/// Create a SQL boolean literal expression (`TRUE` or `FALSE`).
pub fn boolean(value: bool) -> Expr {
    Expr(Expression::Boolean(BooleanLiteral { value }))
}

/// Create a table reference expression.
///
/// The `name` string is split on `.` to determine qualification level:
///
/// - `"table"` -- unqualified table reference
/// - `"schema.table"` -- schema-qualified
/// - `"catalog.schema.table"` -- fully qualified with catalog
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::table;
///
/// let t = table("my_schema.users");
/// assert_eq!(t.to_sql(), "my_schema.users");
/// ```
pub fn table(name: &str) -> Expr {
    Expr(Expression::Table(Box::new(builder_table_ref(name))))
}

/// Create a SQL function call expression.
///
/// `name` is the function name (e.g. `"COUNT"`, `"UPPER"`, `"COALESCE"`), and `args`
/// provides zero or more argument expressions.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::{func, col, star};
///
/// let upper = func("UPPER", [col("name")]);
/// assert_eq!(upper.to_sql(), "UPPER(name)");
///
/// let count = func("COUNT", [star()]);
/// assert_eq!(count.to_sql(), "COUNT(*)");
/// ```
pub fn func(name: &str, args: impl IntoIterator<Item = Expr>) -> Expr {
    Expr(Expression::Function(Box::new(Function {
        name: name.to_string(),
        args: args.into_iter().map(|a| a.0).collect(),
        ..Function::default()
    })))
}

/// Create a `CAST(expr AS type)` expression.
///
/// The `to` parameter is parsed as a data type name. Common built-in types (`INT`, `BIGINT`,
/// `VARCHAR`, `BOOLEAN`, `TIMESTAMP`, etc.) are recognized directly. More complex types
/// (e.g. `"DECIMAL(10,2)"`, `"ARRAY<INT>"`) are parsed via the full SQL parser as a
/// fallback.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::{cast, col};
///
/// let expr = cast(col("id"), "VARCHAR");
/// assert_eq!(expr.to_sql(), "CAST(id AS VARCHAR)");
/// ```
pub fn cast(expr: Expr, to: &str) -> Expr {
    let data_type = parse_simple_data_type(to);
    Expr(Expression::Cast(Box::new(Cast {
        this: expr.0,
        to: data_type,
        trailing_comments: Vec::new(),
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    })))
}

/// Create a `NOT expr` unary expression.
///
/// Wraps the given expression in a logical negation. Equivalent to calling
/// [`Expr::not()`] on the expression.
pub fn not(expr: Expr) -> Expr {
    Expr(Expression::Not(Box::new(UnaryOp::new(expr.0))))
}

/// Combine two expressions with `AND`.
///
/// Equivalent to `left.and(right)`. Useful when you do not have the left-hand side
/// as the receiver.
pub fn and(left: Expr, right: Expr) -> Expr {
    left.and(right)
}

/// Combine two expressions with `OR`.
///
/// Equivalent to `left.or(right)`. Useful when you do not have the left-hand side
/// as the receiver.
pub fn or(left: Expr, right: Expr) -> Expr {
    left.or(right)
}

/// Create an `expr AS name` alias expression.
///
/// This is the free-function form. The method form [`Expr::alias()`] is often more
/// convenient for chaining.
pub fn alias(expr: Expr, name: &str) -> Expr {
    Expr(Expression::Alias(Box::new(Alias {
        this: expr.0,
        alias: builder_identifier(name),
        column_aliases: Vec::new(),
        alias_explicit_as: false,
        alias_keyword: None,
        pre_alias_comments: Vec::new(),
        trailing_comments: Vec::new(),
        inferred_type: None,
    })))
}

/// Parse a raw SQL expression fragment into an [`Expr`].
///
/// Internally wraps the string in `SELECT <sql>`, parses it with the full SQL parser,
/// and extracts the first expression from the SELECT list. This is useful for
/// embedding complex SQL fragments (window functions, subquery predicates, etc.)
/// that would be cumbersome to build purely through the builder API.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::sql_expr;
///
/// let expr = sql_expr("COALESCE(a, b, 0)");
/// assert_eq!(expr.to_sql(), "COALESCE(a, b, 0)");
///
/// let cond = sql_expr("age > 18 AND status = 'active'");
/// ```
///
/// # Panics
///
/// Panics if the SQL fragment cannot be parsed, or if the parser fails to extract a
/// valid expression from the result. Invalid SQL will cause a panic with a message
/// prefixed by `"sql_expr:"`.
pub fn sql_expr(sql: &str) -> Expr {
    let wrapped = format!("SELECT {}", sql);
    let ast = Parser::parse_sql(&wrapped).expect("sql_expr: failed to parse SQL expression");
    if let Expression::Select(s) = &ast[0] {
        if let Some(first) = s.expressions.first() {
            return Expr(first.clone());
        }
    }
    panic!("sql_expr: failed to extract expression from parsed SQL");
}

/// Parse a SQL condition string into an [`Expr`].
///
/// This is a convenience alias for [`sql_expr()`]. The name `condition` reads more
/// naturally when the fragment is intended as a WHERE or HAVING predicate.
///
/// # Panics
///
/// Panics under the same conditions as [`sql_expr()`].
pub fn condition(sql: &str) -> Expr {
    sql_expr(sql)
}

// ---------------------------------------------------------------------------
// Function helpers — typed AST constructors
// ---------------------------------------------------------------------------

// -- Aggregates ---------------------------------------------------------------

/// Create a `COUNT(expr)` expression.
pub fn count(expr: Expr) -> Expr {
    Expr(Expression::Count(Box::new(CountFunc {
        this: Some(expr.0),
        star: false,
        distinct: false,
        filter: None,
        ignore_nulls: None,
        original_name: None,
        inferred_type: None,
    })))
}

/// Create a `COUNT(*)` expression.
pub fn count_star() -> Expr {
    Expr(Expression::Count(Box::new(CountFunc {
        this: None,
        star: true,
        distinct: false,
        filter: None,
        ignore_nulls: None,
        original_name: None,
        inferred_type: None,
    })))
}

/// Create a `COUNT(DISTINCT expr)` expression.
pub fn count_distinct(expr: Expr) -> Expr {
    Expr(Expression::Count(Box::new(CountFunc {
        this: Some(expr.0),
        star: false,
        distinct: true,
        filter: None,
        ignore_nulls: None,
        original_name: None,
        inferred_type: None,
    })))
}

/// Create a `SUM(expr)` expression.
pub fn sum(expr: Expr) -> Expr {
    Expr(Expression::Sum(Box::new(AggFunc {
        this: expr.0,
        distinct: false,
        filter: None,
        order_by: vec![],
        name: None,
        ignore_nulls: None,
        having_max: None,
        limit: None,
        inferred_type: None,
    })))
}

/// Create an `AVG(expr)` expression.
pub fn avg(expr: Expr) -> Expr {
    Expr(Expression::Avg(Box::new(AggFunc {
        this: expr.0,
        distinct: false,
        filter: None,
        order_by: vec![],
        name: None,
        ignore_nulls: None,
        having_max: None,
        limit: None,
        inferred_type: None,
    })))
}

/// Create a `MIN(expr)` expression. Named `min_` to avoid conflict with `std::cmp::min`.
pub fn min_(expr: Expr) -> Expr {
    Expr(Expression::Min(Box::new(AggFunc {
        this: expr.0,
        distinct: false,
        filter: None,
        order_by: vec![],
        name: None,
        ignore_nulls: None,
        having_max: None,
        limit: None,
        inferred_type: None,
    })))
}

/// Create a `MAX(expr)` expression. Named `max_` to avoid conflict with `std::cmp::max`.
pub fn max_(expr: Expr) -> Expr {
    Expr(Expression::Max(Box::new(AggFunc {
        this: expr.0,
        distinct: false,
        filter: None,
        order_by: vec![],
        name: None,
        ignore_nulls: None,
        having_max: None,
        limit: None,
        inferred_type: None,
    })))
}

/// Create an `APPROX_DISTINCT(expr)` expression.
pub fn approx_distinct(expr: Expr) -> Expr {
    Expr(Expression::ApproxDistinct(Box::new(AggFunc {
        this: expr.0,
        distinct: false,
        filter: None,
        order_by: vec![],
        name: None,
        ignore_nulls: None,
        having_max: None,
        limit: None,
        inferred_type: None,
    })))
}

// -- String functions ---------------------------------------------------------

/// Create an `UPPER(expr)` expression.
pub fn upper(expr: Expr) -> Expr {
    Expr(Expression::Upper(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `LOWER(expr)` expression.
pub fn lower(expr: Expr) -> Expr {
    Expr(Expression::Lower(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `LENGTH(expr)` expression.
pub fn length(expr: Expr) -> Expr {
    Expr(Expression::Length(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `TRIM(expr)` expression.
pub fn trim(expr: Expr) -> Expr {
    Expr(Expression::Trim(Box::new(TrimFunc {
        this: expr.0,
        characters: None,
        position: TrimPosition::Both,
        sql_standard_syntax: false,
        position_explicit: false,
        inferred_type: None,
    })))
}

/// Create an `LTRIM(expr)` expression.
pub fn ltrim(expr: Expr) -> Expr {
    Expr(Expression::LTrim(Box::new(UnaryFunc::new(expr.0))))
}

/// Create an `RTRIM(expr)` expression.
pub fn rtrim(expr: Expr) -> Expr {
    Expr(Expression::RTrim(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `REVERSE(expr)` expression.
pub fn reverse(expr: Expr) -> Expr {
    Expr(Expression::Reverse(Box::new(UnaryFunc::new(expr.0))))
}

/// Create an `INITCAP(expr)` expression.
pub fn initcap(expr: Expr) -> Expr {
    Expr(Expression::Initcap(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `SUBSTRING(expr, start, len)` expression.
pub fn substring(expr: Expr, start: Expr, len: Option<Expr>) -> Expr {
    Expr(Expression::Substring(Box::new(SubstringFunc {
        this: expr.0,
        start: start.0,
        length: len.map(|l| l.0),
        from_for_syntax: false,
    })))
}

/// Create a `REPLACE(expr, old, new)` expression. Named `replace_` to avoid
/// conflict with the `str::replace` method.
pub fn replace_(expr: Expr, old: Expr, new: Expr) -> Expr {
    Expr(Expression::Replace(Box::new(ReplaceFunc {
        this: expr.0,
        old: old.0,
        new: new.0,
    })))
}

/// Create a `CONCAT_WS(separator, exprs...)` expression.
pub fn concat_ws(separator: Expr, exprs: impl IntoIterator<Item = Expr>) -> Expr {
    Expr(Expression::ConcatWs(Box::new(ConcatWs {
        separator: separator.0,
        expressions: exprs.into_iter().map(|e| e.0).collect(),
    })))
}

// -- Null handling ------------------------------------------------------------

/// Create a `COALESCE(exprs...)` expression.
pub fn coalesce(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    Expr(Expression::Coalesce(Box::new(VarArgFunc {
        expressions: exprs.into_iter().map(|e| e.0).collect(),
        original_name: None,
        inferred_type: None,
    })))
}

/// Create a `NULLIF(expr1, expr2)` expression.
pub fn null_if(expr1: Expr, expr2: Expr) -> Expr {
    Expr(Expression::NullIf(Box::new(BinaryFunc {
        this: expr1.0,
        expression: expr2.0,
        original_name: None,
        inferred_type: None,
    })))
}

/// Create an `IFNULL(expr, fallback)` expression.
pub fn if_null(expr: Expr, fallback: Expr) -> Expr {
    Expr(Expression::IfNull(Box::new(BinaryFunc {
        this: expr.0,
        expression: fallback.0,
        original_name: None,
        inferred_type: None,
    })))
}

// -- Math functions -----------------------------------------------------------

/// Create an `ABS(expr)` expression.
pub fn abs(expr: Expr) -> Expr {
    Expr(Expression::Abs(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `ROUND(expr, decimals)` expression.
pub fn round(expr: Expr, decimals: Option<Expr>) -> Expr {
    Expr(Expression::Round(Box::new(RoundFunc {
        this: expr.0,
        decimals: decimals.map(|d| d.0),
    })))
}

/// Create a `FLOOR(expr)` expression.
pub fn floor(expr: Expr) -> Expr {
    Expr(Expression::Floor(Box::new(FloorFunc {
        this: expr.0,
        scale: None,
        to: None,
    })))
}

/// Create a `CEIL(expr)` expression.
pub fn ceil(expr: Expr) -> Expr {
    Expr(Expression::Ceil(Box::new(CeilFunc {
        this: expr.0,
        decimals: None,
        to: None,
    })))
}

/// Create a `POWER(base, exp)` expression.
pub fn power(base: Expr, exponent: Expr) -> Expr {
    Expr(Expression::Power(Box::new(BinaryFunc {
        this: base.0,
        expression: exponent.0,
        original_name: None,
        inferred_type: None,
    })))
}

/// Create a `SQRT(expr)` expression.
pub fn sqrt(expr: Expr) -> Expr {
    Expr(Expression::Sqrt(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `LN(expr)` expression.
pub fn ln(expr: Expr) -> Expr {
    Expr(Expression::Ln(Box::new(UnaryFunc::new(expr.0))))
}

/// Create an `EXP(expr)` expression. Named `exp_` to avoid conflict with `std::f64::consts`.
pub fn exp_(expr: Expr) -> Expr {
    Expr(Expression::Exp(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `SIGN(expr)` expression.
pub fn sign(expr: Expr) -> Expr {
    Expr(Expression::Sign(Box::new(UnaryFunc::new(expr.0))))
}

/// Create a `GREATEST(exprs...)` expression.
pub fn greatest(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    Expr(Expression::Greatest(Box::new(VarArgFunc {
        expressions: exprs.into_iter().map(|e| e.0).collect(),
        original_name: None,
        inferred_type: None,
    })))
}

/// Create a `LEAST(exprs...)` expression.
pub fn least(exprs: impl IntoIterator<Item = Expr>) -> Expr {
    Expr(Expression::Least(Box::new(VarArgFunc {
        expressions: exprs.into_iter().map(|e| e.0).collect(),
        original_name: None,
        inferred_type: None,
    })))
}

// -- Date/time functions ------------------------------------------------------

/// Create a `CURRENT_DATE` expression.
pub fn current_date_() -> Expr {
    Expr(Expression::CurrentDate(CurrentDate))
}

/// Create a `CURRENT_TIME` expression.
pub fn current_time_() -> Expr {
    Expr(Expression::CurrentTime(CurrentTime { precision: None }))
}

/// Create a `CURRENT_TIMESTAMP` expression.
pub fn current_timestamp_() -> Expr {
    Expr(Expression::CurrentTimestamp(CurrentTimestamp {
        precision: None,
        sysdate: false,
    }))
}

/// Create an `EXTRACT(field FROM expr)` expression.
pub fn extract_(field: &str, expr: Expr) -> Expr {
    Expr(Expression::Extract(Box::new(ExtractFunc {
        this: expr.0,
        field: parse_datetime_field(field),
    })))
}

/// Parse a datetime field name string into a [`DateTimeField`] enum value.
fn parse_datetime_field(field: &str) -> DateTimeField {
    match field.to_uppercase().as_str() {
        "YEAR" => DateTimeField::Year,
        "MONTH" => DateTimeField::Month,
        "DAY" => DateTimeField::Day,
        "HOUR" => DateTimeField::Hour,
        "MINUTE" => DateTimeField::Minute,
        "SECOND" => DateTimeField::Second,
        "MILLISECOND" => DateTimeField::Millisecond,
        "MICROSECOND" => DateTimeField::Microsecond,
        "DOW" | "DAYOFWEEK" => DateTimeField::DayOfWeek,
        "DOY" | "DAYOFYEAR" => DateTimeField::DayOfYear,
        "WEEK" => DateTimeField::Week,
        "QUARTER" => DateTimeField::Quarter,
        "EPOCH" => DateTimeField::Epoch,
        "TIMEZONE" => DateTimeField::Timezone,
        "TIMEZONE_HOUR" => DateTimeField::TimezoneHour,
        "TIMEZONE_MINUTE" => DateTimeField::TimezoneMinute,
        "DATE" => DateTimeField::Date,
        "TIME" => DateTimeField::Time,
        other => DateTimeField::Custom(other.to_string()),
    }
}

// -- Window functions ---------------------------------------------------------

/// Create a `ROW_NUMBER()` expression.
pub fn row_number() -> Expr {
    Expr(Expression::RowNumber(RowNumber))
}

/// Create a `RANK()` expression. Named `rank_` to avoid confusion with `Rank` struct.
pub fn rank_() -> Expr {
    Expr(Expression::Rank(Rank {
        order_by: None,
        args: vec![],
    }))
}

/// Create a `DENSE_RANK()` expression.
pub fn dense_rank() -> Expr {
    Expr(Expression::DenseRank(DenseRank { args: vec![] }))
}

// ---------------------------------------------------------------------------
// Query starters
// ---------------------------------------------------------------------------

/// Start building a SELECT query with the given column expressions.
///
/// Accepts any iterable of items implementing [`IntoExpr`], which includes `&str`
/// (interpreted as column names), [`Expr`] values, and raw [`Expression`] nodes.
/// Returns a [`SelectBuilder`] that can be further refined with `.from()`, `.where_()`,
/// `.order_by()`, etc.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// // Using string slices (converted to column refs automatically)
/// let sql = select(["id", "name"]).from("users").to_sql();
/// assert_eq!(sql, "SELECT id, name FROM users");
///
/// // Using Expr values for computed columns
/// let sql = select([col("price").mul(col("qty")).alias("total")])
///     .from("items")
///     .to_sql();
/// assert_eq!(sql, "SELECT price * qty AS total FROM items");
/// ```
pub fn select<I, E>(expressions: I) -> SelectBuilder
where
    I: IntoIterator<Item = E>,
    E: IntoExpr,
{
    SelectBuilder::new().select_cols(expressions)
}

/// Start building a SELECT query beginning with a FROM clause.
///
/// Returns a [`SelectBuilder`] with the FROM clause already set. Use
/// [`SelectBuilder::select_cols()`] to add columns afterward. This is an alternative
/// entry point for queries where specifying the table first feels more natural.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = from("users").select_cols(["id", "name"]).to_sql();
/// assert_eq!(sql, "SELECT id, name FROM users");
/// ```
pub fn from(table_name: &str) -> SelectBuilder {
    SelectBuilder::new().from(table_name)
}

/// Start building a `DELETE FROM` statement targeting the given table.
///
/// Returns a [`DeleteBuilder`] which supports `.where_()` to add a predicate.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = delete("users").where_(col("id").eq(lit(1))).to_sql();
/// assert_eq!(sql, "DELETE FROM users WHERE id = 1");
/// ```
pub fn delete(table_name: &str) -> DeleteBuilder {
    DeleteBuilder {
        delete: Delete {
            table: builder_table_ref(table_name),
            hint: None,
            on_cluster: None,
            alias: None,
            alias_explicit_as: false,
            using: Vec::new(),
            where_clause: None,
            output: None,
            leading_comments: Vec::new(),
            with: None,
            limit: None,
            order_by: None,
            returning: Vec::new(),
            tables: Vec::new(),
            tables_from_using: false,
            joins: Vec::new(),
            force_index: None,
            no_from: false,
        },
    }
}

/// Start building an `INSERT INTO` statement targeting the given table.
///
/// Returns an [`InsertBuilder`] which supports `.columns()`, `.values()`, and
/// `.query()` for INSERT ... SELECT.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = insert_into("users")
///     .columns(["id", "name"])
///     .values([lit(1), lit("Alice")])
///     .to_sql();
/// assert_eq!(sql, "INSERT INTO users (id, name) VALUES (1, 'Alice')");
/// ```
pub fn insert_into(table_name: &str) -> InsertBuilder {
    InsertBuilder {
        insert: Insert {
            table: builder_table_ref(table_name),
            columns: Vec::new(),
            values: Vec::new(),
            query: None,
            overwrite: false,
            partition: Vec::new(),
            directory: None,
            returning: Vec::new(),
            output: None,
            on_conflict: None,
            leading_comments: Vec::new(),
            if_exists: false,
            with: None,
            ignore: false,
            source_alias: None,
            alias: None,
            alias_explicit_as: false,
            default_values: false,
            by_name: false,
            conflict_action: None,
            is_replace: false,
            hint: None,
            replace_where: None,
            source: None,
            function_target: None,
            partition_by: None,
            settings: Vec::new(),
        },
    }
}

/// Start building an `UPDATE` statement targeting the given table.
///
/// Returns an [`UpdateBuilder`] which supports `.set()` for column assignments,
/// `.where_()` for predicates, and `.from()` for PostgreSQL/Snowflake-style
/// UPDATE ... FROM syntax.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = update("users")
///     .set("name", lit("Bob"))
///     .where_(col("id").eq(lit(1)))
///     .to_sql();
/// assert_eq!(sql, "UPDATE users SET name = 'Bob' WHERE id = 1");
/// ```
pub fn update(table_name: &str) -> UpdateBuilder {
    UpdateBuilder {
        update: Update {
            table: builder_table_ref(table_name),
            hint: None,
            extra_tables: Vec::new(),
            table_joins: Vec::new(),
            set: Vec::new(),
            from_clause: None,
            from_joins: Vec::new(),
            where_clause: None,
            returning: Vec::new(),
            output: None,
            with: None,
            leading_comments: Vec::new(),
            limit: None,
            order_by: None,
            from_before_set: false,
        },
    }
}

// ---------------------------------------------------------------------------
// Expr wrapper (for operator methods)
// ---------------------------------------------------------------------------

/// A thin wrapper around [`Expression`] that provides fluent operator methods.
///
/// `Expr` is the primary value type flowing through the builder API. It wraps a single
/// AST [`Expression`] node and adds convenience methods for comparisons (`.eq()`,
/// `.gt()`, etc.), logical connectives (`.and()`, `.or()`, `.not()`), arithmetic
/// (`.add()`, `.sub()`, `.mul()`, `.div()`), pattern matching (`.like()`, `.ilike()`,
/// `.rlike()`), and other SQL operations (`.in_list()`, `.between()`, `.is_null()`,
/// `.alias()`, `.cast()`, `.asc()`, `.desc()`).
///
/// The inner [`Expression`] is publicly accessible via the `.0` field or
/// [`Expr::into_inner()`].
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let condition = col("age").gte(lit(18)).and(col("active").eq(boolean(true)));
/// assert_eq!(condition.to_sql(), "age >= 18 AND active = TRUE");
/// ```
#[derive(Debug, Clone)]
pub struct Expr(pub Expression);

impl Expr {
    /// Consume this wrapper and return the inner [`Expression`] node.
    pub fn into_inner(self) -> Expression {
        self.0
    }

    /// Generate a SQL string from this expression using the default (generic) dialect.
    ///
    /// Returns an empty string if generation fails.
    pub fn to_sql(&self) -> String {
        generate_builder_sql(&self.0)
    }

    // -- Comparison operators --

    /// Produce a `self = other` equality comparison.
    pub fn eq(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Eq, self.0, other.0))
    }

    /// Produce a `self <> other` inequality comparison.
    pub fn neq(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Neq, self.0, other.0))
    }

    /// Produce a `self < other` less-than comparison.
    pub fn lt(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Lt, self.0, other.0))
    }

    /// Produce a `self <= other` less-than-or-equal comparison.
    pub fn lte(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Lte, self.0, other.0))
    }

    /// Produce a `self > other` greater-than comparison.
    pub fn gt(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Gt, self.0, other.0))
    }

    /// Produce a `self >= other` greater-than-or-equal comparison.
    pub fn gte(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Gte, self.0, other.0))
    }

    // -- Logical operators --

    /// Produce a `self AND other` logical conjunction.
    pub fn and(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::And, self.0, other.0))
    }

    /// Produce a `self OR other` logical disjunction.
    pub fn or(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Or, self.0, other.0))
    }

    /// Produce a `NOT self` logical negation.
    pub fn not(self) -> Expr {
        Expr(engine::unary(engine::UnaryKind::Not, self.0))
    }

    /// Produce a `self XOR other` logical exclusive-or.
    pub fn xor(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Xor, self.0, other.0))
    }

    // -- Arithmetic operators --

    /// Produce a `self + other` addition expression.
    pub fn add(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Add, self.0, other.0))
    }

    /// Produce a `self - other` subtraction expression.
    pub fn sub(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Sub, self.0, other.0))
    }

    /// Produce a `self * other` multiplication expression.
    pub fn mul(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Mul, self.0, other.0))
    }

    /// Produce a `self / other` division expression.
    pub fn div(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Div, self.0, other.0))
    }

    /// Produce a `self % other` modulo expression.
    pub fn modulo(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Mod, self.0, other.0))
    }

    /// Produce an arithmetic negation expression.
    pub fn neg(self) -> Expr {
        Expr(engine::unary(engine::UnaryKind::Neg, self.0))
    }

    /// Produce a generic `self IS other` predicate.
    pub fn is(self, other: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Is, self.0, other.0))
    }

    // -- Other operators --

    /// Produce a `self IS NULL` predicate.
    pub fn is_null(self) -> Expr {
        Expr(engine::unary(engine::UnaryKind::IsNull, self.0))
    }

    /// Produce a `self IS NOT NULL` predicate (implemented as `NOT (self IS NULL)`).
    pub fn is_not_null(self) -> Expr {
        Expr(engine::unary(engine::UnaryKind::IsNotNull, self.0))
    }

    /// Produce a `self IN (values...)` membership test.
    ///
    /// Each element of `values` becomes an item in the parenthesized list.
    pub fn in_list(self, values: impl IntoIterator<Item = Expr>) -> Expr {
        Expr(Expression::In(Box::new(In {
            this: self.0,
            expressions: values.into_iter().map(|v| v.0).collect(),
            query: None,
            not: false,
            global: false,
            unnest: None,
            is_field: false,
        })))
    }

    /// Produce a `self BETWEEN low AND high` range test.
    pub fn between(self, low: Expr, high: Expr) -> Expr {
        Expr(Expression::Between(Box::new(Between {
            this: self.0,
            low: low.0,
            high: high.0,
            not: false,
            symmetric: None,
        })))
    }

    /// Produce a `self LIKE pattern` case-sensitive pattern match.
    pub fn like(self, pattern: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::Like, self.0, pattern.0))
    }

    /// Produce a `self AS alias` expression alias.
    pub fn alias(self, name: &str) -> Expr {
        alias(self, name)
    }

    /// Produce a `CAST(self AS type)` type conversion.
    ///
    /// The `to` parameter is parsed as a data type name; see [`cast()`] for details.
    pub fn cast(self, to: &str) -> Expr {
        cast(self, to)
    }

    /// Wrap this expression with ascending sort order (`self ASC`).
    ///
    /// Used in ORDER BY clauses. Expressions without an explicit `.asc()` or `.desc()`
    /// call default to ascending order when passed to [`SelectBuilder::order_by()`].
    pub fn asc(self) -> Expr {
        Expr(Expression::Ordered(Box::new(Ordered {
            nulls_auto: false,
            this: self.0,
            desc: false,
            nulls_first: None,
            explicit_asc: true,
            with_fill: None,
        })))
    }

    /// Wrap this expression with descending sort order (`self DESC`).
    ///
    /// Used in ORDER BY clauses.
    pub fn desc(self) -> Expr {
        Expr(Expression::Ordered(Box::new(Ordered {
            nulls_auto: false,
            this: self.0,
            desc: true,
            nulls_first: None,
            explicit_asc: false,
            with_fill: None,
        })))
    }

    /// Produce a `self ILIKE pattern` case-insensitive pattern match.
    ///
    /// Supported by PostgreSQL, Snowflake, and other dialects. Dialects that do not
    /// support `ILIKE` natively may need transpilation.
    pub fn ilike(self, pattern: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::ILike, self.0, pattern.0))
    }

    /// Produce a `REGEXP_LIKE(self, pattern)` regular expression match.
    ///
    /// The generated SQL uses the `REGEXP_LIKE` function form. Different dialects may
    /// render this as `RLIKE`, `REGEXP`, or `REGEXP_LIKE` after transpilation.
    pub fn rlike(self, pattern: Expr) -> Expr {
        Expr(engine::binary(engine::BinaryKind::RLike, self.0, pattern.0))
    }

    /// Produce a `self NOT IN (values...)` negated membership test.
    ///
    /// Each element of `values` becomes an item in the parenthesized list.
    pub fn not_in(self, values: impl IntoIterator<Item = Expr>) -> Expr {
        Expr(Expression::In(Box::new(In {
            this: self.0,
            expressions: values.into_iter().map(|v| v.0).collect(),
            query: None,
            not: true,
            global: false,
            unnest: None,
            is_field: false,
        })))
    }
}

// ---------------------------------------------------------------------------
// SelectBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for constructing `SELECT` statements.
///
/// Created by the [`select()`] or [`from()`] entry-point functions. Methods on this
/// builder return `self` so they can be chained. Call [`.build()`](SelectBuilder::build)
/// to obtain an [`Expression`], or [`.to_sql()`](SelectBuilder::to_sql) to generate a
/// SQL string directly.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = select(["u.id", "u.name"])
///     .from("users")
///     .left_join("orders", col("u.id").eq(col("o.user_id")))
///     .where_(col("u.active").eq(boolean(true)))
///     .group_by(["u.id", "u.name"])
///     .order_by([col("u.name").asc()])
///     .limit(100)
///     .to_sql();
/// ```
pub struct SelectBuilder {
    select: Select,
}

impl SelectBuilder {
    fn new() -> Self {
        SelectBuilder {
            select: Select::new(),
        }
    }

    fn edit(mut self, edit: impl FnOnce(&mut Expression)) -> Self {
        let mut expression = Expression::Select(Box::new(self.select));
        edit(&mut expression);
        self.select = match expression {
            Expression::Select(select) => *select,
            _ => unreachable!("select builder engine changed the expression kind"),
        };
        self
    }

    fn join_with_kind(self, table_name: &str, on: Option<Expr>, kind: JoinKind) -> Self {
        let join = Join {
            kind,
            this: Expression::Table(Box::new(builder_table_ref(table_name))),
            on: on.map(|expression| expression.0),
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
        };
        self.edit(|expression| {
            engine::append_join(expression, join).expect("select builder accepts JOIN clauses")
        })
    }

    /// Append columns to the SELECT list.
    ///
    /// Accepts any iterable of [`IntoExpr`] items. This is primarily useful when the
    /// builder was created via [`from()`] and columns need to be added afterward.
    pub fn select_cols<I, E>(self, expressions: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        self.select_cols_with_options(expressions, ClauseOptions::default())
    }

    /// Add SELECT expressions, optionally replacing the current projection.
    pub fn select_cols_with_options<I, E>(self, expressions: I, options: ClauseOptions) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        let values = expressions
            .into_iter()
            .map(|expression| expression.into_expr().0)
            .collect();
        self.edit(|expression| {
            engine::append_select(expression, values, options.append)
                .expect("select builder accepts SELECT clauses")
        })
    }

    /// Set the FROM clause to reference the given table by name.
    pub fn from(self, table_name: &str) -> Self {
        self.edit(|expression| {
            engine::set_from(
                expression,
                vec![Expression::Table(Box::new(builder_table_ref(table_name)))],
            )
            .expect("select builder accepts FROM clauses")
        })
    }

    /// Set the FROM clause to an arbitrary expression (e.g. a subquery or table function).
    ///
    /// Use this instead of [`SelectBuilder::from()`] when the source is not a simple
    /// table name -- for example, a [`subquery()`] or a table-valued function.
    pub fn from_expr(self, expr: Expr) -> Self {
        self.edit(|expression| {
            engine::set_from(expression, vec![expr.0]).expect("select builder accepts FROM clauses")
        })
    }

    /// Add an inner `JOIN` clause with the given ON condition.
    pub fn join(self, table_name: &str, on: Expr) -> Self {
        self.join_with_kind(table_name, Some(on), JoinKind::Inner)
    }

    /// Add a `LEFT JOIN` clause with the given ON condition.
    pub fn left_join(self, table_name: &str, on: Expr) -> Self {
        self.join_with_kind(table_name, Some(on), JoinKind::Left)
    }

    /// Add a predicate to the WHERE clause, combining repeated calls with `AND`.
    pub fn where_(self, condition: Expr) -> Self {
        self.where_with_options(condition, ClauseOptions::default())
    }

    /// Add or replace the WHERE clause according to `options.append`.
    pub fn where_with_options(self, condition: Expr, options: ClauseOptions) -> Self {
        self.edit(|expression| {
            engine::apply_where(expression, condition.0, options.append)
                .expect("select builder accepts WHERE clauses")
        })
    }

    /// Set the GROUP BY clause with the given grouping expressions.
    pub fn group_by<I, E>(self, expressions: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        self.group_by_with_options(expressions, ClauseOptions::default())
    }

    /// Add or replace grouping expressions according to `options.append`.
    pub fn group_by_with_options<I, E>(self, expressions: I, options: ClauseOptions) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        let values = expressions
            .into_iter()
            .map(|expression| expression.into_expr().0)
            .collect();
        self.edit(|expression| {
            engine::apply_group_by(expression, values, options.append)
                .expect("select builder accepts GROUP BY clauses")
        })
    }

    /// Set the HAVING clause to filter groups by the given condition.
    pub fn having(self, condition: Expr) -> Self {
        self.having_with_options(condition, ClauseOptions::default())
    }

    /// Add or replace the HAVING clause according to `options.append`.
    pub fn having_with_options(self, condition: Expr, options: ClauseOptions) -> Self {
        self.edit(|expression| {
            engine::apply_having(expression, condition.0, options.append)
                .expect("select builder accepts HAVING clauses")
        })
    }

    /// Set the ORDER BY clause with the given sort expressions.
    ///
    /// Expressions that are not already wrapped with [`.asc()`](Expr::asc) or
    /// [`.desc()`](Expr::desc) default to ascending order. String values are
    /// interpreted as column names via [`IntoExpr`].
    pub fn order_by<I, E>(self, expressions: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        self.order_by_with_options(expressions, ClauseOptions::default())
    }

    /// Add or replace ordering expressions according to `options.append`.
    pub fn order_by_with_options<I, E>(self, expressions: I, options: ClauseOptions) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        let values = expressions
            .into_iter()
            .map(|expression| engine::ordered(expression.into_expr().0))
            .collect();
        self.edit(|expression| {
            engine::apply_order_by(expression, values, options.append)
                .expect("select builder accepts ORDER BY clauses")
        })
    }

    /// Set the SORT BY clause with the given sort expressions.
    ///
    /// SORT BY is used in Hive/Spark to sort data within each reducer (partition),
    /// as opposed to ORDER BY which sorts globally. Expressions that are not already
    /// wrapped with [`.asc()`](Expr::asc) or [`.desc()`](Expr::desc) default to
    /// ascending order.
    pub fn sort_by<I, E>(self, expressions: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        self.sort_by_with_options(expressions, ClauseOptions::default())
    }

    /// Add or replace SORT BY expressions according to `options.append`.
    pub fn sort_by_with_options<I, E>(self, expressions: I, options: ClauseOptions) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        let values = expressions
            .into_iter()
            .map(|expression| engine::ordered(expression.into_expr().0))
            .collect();
        self.edit(|expression| {
            engine::apply_sort_by(expression, values, options.append)
                .expect("select builder accepts SORT BY clauses")
        })
    }

    /// Set the LIMIT clause to restrict the result set to `count` rows.
    pub fn limit(self, count: usize) -> Self {
        self.edit(|expression| {
            engine::apply_limit(
                expression,
                Expression::Literal(Box::new(Literal::Number(count.to_string()))),
            )
            .expect("select builder accepts LIMIT clauses")
        })
    }

    /// Set the OFFSET clause to skip the first `count` rows.
    pub fn offset(self, count: usize) -> Self {
        self.edit(|expression| {
            engine::apply_offset(
                expression,
                Expression::Literal(Box::new(Literal::Number(count.to_string()))),
            )
            .expect("select builder accepts OFFSET clauses")
        })
    }

    /// Enable the DISTINCT modifier on the SELECT clause.
    pub fn distinct(self) -> Self {
        self.edit(|expression| {
            engine::apply_distinct(expression, true)
                .expect("select builder accepts DISTINCT clauses")
        })
    }

    /// Add a QUALIFY clause to filter rows after window function evaluation.
    ///
    /// QUALIFY is supported by Snowflake, BigQuery, DuckDB, and Databricks. It acts
    /// like a WHERE clause but is applied after window functions are computed.
    pub fn qualify(self, condition: Expr) -> Self {
        self.qualify_with_options(condition, ClauseOptions::default())
    }

    /// Add or replace the QUALIFY clause according to `options.append`.
    pub fn qualify_with_options(self, condition: Expr, options: ClauseOptions) -> Self {
        self.edit(|expression| {
            engine::apply_qualify(expression, condition.0, options.append)
                .expect("select builder accepts QUALIFY clauses")
        })
    }

    /// Add a `RIGHT JOIN` clause with the given ON condition.
    pub fn right_join(self, table_name: &str, on: Expr) -> Self {
        self.join_with_kind(table_name, Some(on), JoinKind::Right)
    }

    /// Add a `FULL JOIN` clause with the given ON condition.
    pub fn full_join(self, table_name: &str, on: Expr) -> Self {
        self.join_with_kind(table_name, Some(on), JoinKind::Full)
    }

    /// Add a `CROSS JOIN` clause (Cartesian product, no ON condition).
    pub fn cross_join(self, table_name: &str) -> Self {
        self.join_with_kind(table_name, None, JoinKind::Cross)
    }

    /// Add a `LATERAL VIEW` clause for Hive/Spark user-defined table function (UDTF)
    /// expansion.
    ///
    /// `table_function` is the UDTF expression (e.g. `func("EXPLODE", [col("arr")])`),
    /// `table_alias` names the virtual table, and `column_aliases` name the output
    /// columns produced by the function.
    pub fn lateral_view<S: AsRef<str>>(
        self,
        table_function: Expr,
        table_alias: &str,
        column_aliases: impl IntoIterator<Item = S>,
    ) -> Self {
        self.lateral_view_with_options(
            table_function,
            table_alias,
            column_aliases,
            LateralViewOptions::default(),
        )
    }

    /// Add a `LATERAL VIEW` clause with options such as `OUTER`.
    pub fn lateral_view_with_options<S: AsRef<str>>(
        self,
        table_function: Expr,
        table_alias: &str,
        column_aliases: impl IntoIterator<Item = S>,
        options: LateralViewOptions,
    ) -> Self {
        let aliases = column_aliases
            .into_iter()
            .map(|c| builder_identifier(c.as_ref()))
            .collect();
        self.edit(|expression| {
            engine::append_lateral_view(
                expression,
                table_function.0,
                Some(builder_identifier(table_alias)),
                aliases,
                options.outer,
            )
            .expect("select builder accepts LATERAL VIEW clauses")
        })
    }

    /// Add a named `WINDOW` clause definition.
    ///
    /// The window `name` can then be referenced in window function OVER clauses
    /// elsewhere in the query. The definition is constructed via [`WindowDefBuilder`].
    /// Multiple calls append additional named windows.
    pub fn window(self, name: &str, def: WindowDefBuilder) -> Self {
        let order_by = def.order_by;
        self.edit(|expression| {
            engine::append_window(
                expression,
                builder_identifier(name),
                def.partition_by,
                order_by,
            )
            .expect("select builder accepts WINDOW clauses")
        })
    }

    /// Add a `FOR UPDATE` locking clause.
    ///
    /// Appends a `FOR UPDATE` lock to the SELECT statement. This is used by
    /// databases (PostgreSQL, MySQL, Oracle) to lock selected rows for update.
    pub fn for_update(self) -> Self {
        self.edit(|expression| {
            engine::append_lock(expression, engine::LockKind::Update)
                .expect("select builder accepts locking clauses")
        })
    }

    /// Add a `FOR SHARE` locking clause.
    ///
    /// Appends a `FOR SHARE` lock to the SELECT statement. This allows other
    /// transactions to read the locked rows but prevents updates.
    pub fn for_share(self) -> Self {
        self.edit(|expression| {
            engine::append_lock(expression, engine::LockKind::Share)
                .expect("select builder accepts locking clauses")
        })
    }

    /// Add a query hint (e.g., Oracle `/*+ FULL(t) */`).
    ///
    /// Hints are rendered for Oracle, MySQL, Spark, Hive, Databricks, and PostgreSQL
    /// dialects. Multiple calls append additional hints.
    pub fn hint(self, hint_text: &str) -> Self {
        self.edit(|expression| {
            engine::append_hint(expression, hint_text.to_string())
                .expect("select builder accepts query hints")
        })
    }

    /// Convert this SELECT into a `CREATE TABLE AS SELECT` statement.
    ///
    /// Consumes the builder and returns an [`Expression::CreateTable`] with this
    /// query as the `as_select` source.
    ///
    /// # Examples
    ///
    /// ```
    /// use polyglot_sql::builder::*;
    ///
    /// let sql = polyglot_sql::generator::Generator::sql(
    ///     &select(["*"]).from("t").ctas("new_table")
    /// ).unwrap();
    /// assert_eq!(sql, "CREATE TABLE new_table AS SELECT * FROM t");
    /// ```
    pub fn ctas(self, table_name: &str) -> Expression {
        self.ctas_with_options(table_name, CtasOptions::default())
    }

    /// Build a `CREATE TABLE AS SELECT` statement with replacement/temporary options.
    pub fn ctas_with_options(self, table_name: &str, options: CtasOptions) -> Expression {
        engine::create_table_as(
            self.build(),
            builder_table_ref(table_name),
            options.replace,
            options.temporary,
        )
        .expect("select builder CTAS source is a query")
    }

    /// Combine this SELECT with another via `UNION` (duplicate elimination).
    ///
    /// Returns a [`SetOpBuilder`] for further chaining (e.g. `.order_by()`, `.limit()`).
    pub fn union(self, other: SelectBuilder) -> SetOpBuilder {
        SetOpBuilder::new(SetOpKind::Union, self, other, false)
    }

    /// Combine this SELECT with another via `UNION ALL` (keep duplicates).
    ///
    /// Returns a [`SetOpBuilder`] for further chaining.
    pub fn union_all(self, other: SelectBuilder) -> SetOpBuilder {
        SetOpBuilder::new(SetOpKind::Union, self, other, true)
    }

    /// Combine this SELECT with another via `INTERSECT` (rows common to both).
    ///
    /// Returns a [`SetOpBuilder`] for further chaining.
    pub fn intersect(self, other: SelectBuilder) -> SetOpBuilder {
        SetOpBuilder::new(SetOpKind::Intersect, self, other, false)
    }

    /// Combine this SELECT with another via `EXCEPT` (rows in left but not right).
    ///
    /// Returns a [`SetOpBuilder`] for further chaining.
    pub fn except_(self, other: SelectBuilder) -> SetOpBuilder {
        SetOpBuilder::new(SetOpKind::Except, self, other, false)
    }

    /// Consume this builder and produce the final [`Expression::Select`] AST node.
    pub fn build(self) -> Expression {
        Expression::Select(Box::new(self.select))
    }

    /// Consume this builder, generate, and return the SQL string.
    ///
    /// Uses canonical builder rendering (including infix `NOT IN`) and returns an empty
    /// string if generation fails.
    pub fn to_sql(self) -> String {
        generate_builder_sql(&self.build())
    }
}

// ---------------------------------------------------------------------------
// DeleteBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for constructing `DELETE FROM` statements.
///
/// Created by the [`delete()`] entry-point function. Supports an optional `.where_()`
/// predicate.
pub struct DeleteBuilder {
    delete: Delete,
}

impl DeleteBuilder {
    /// Add a predicate to the WHERE clause, combining repeated calls with `AND`.
    pub fn where_(self, condition: Expr) -> Self {
        self.where_with_options(condition, ClauseOptions::default())
    }

    /// Add or replace the WHERE clause according to `options.append`.
    pub fn where_with_options(mut self, condition: Expr, options: ClauseOptions) -> Self {
        let mut expression = Expression::Delete(Box::new(self.delete));
        engine::apply_where(&mut expression, condition.0, options.append)
            .expect("delete builder accepts WHERE clauses");
        self.delete = match expression {
            Expression::Delete(delete) => *delete,
            _ => unreachable!("delete builder engine changed the expression kind"),
        };
        self
    }

    /// Consume this builder and produce the final [`Expression::Delete`] AST node.
    pub fn build(self) -> Expression {
        Expression::Delete(Box::new(self.delete))
    }

    /// Consume this builder, generate, and return the SQL string.
    pub fn to_sql(self) -> String {
        generate_builder_sql(&self.build())
    }
}

// ---------------------------------------------------------------------------
// InsertBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for constructing `INSERT INTO` statements.
///
/// Created by the [`insert_into()`] entry-point function. Supports specifying target
/// columns via [`.columns()`](InsertBuilder::columns), row values via
/// [`.values()`](InsertBuilder::values) (can be called multiple times for multiple rows),
/// and INSERT ... SELECT via [`.query()`](InsertBuilder::query).
pub struct InsertBuilder {
    insert: Insert,
}

impl InsertBuilder {
    fn edit(mut self, edit: impl FnOnce(&mut Expression)) -> Self {
        let mut expression = Expression::Insert(Box::new(self.insert));
        edit(&mut expression);
        self.insert = match expression {
            Expression::Insert(insert) => *insert,
            _ => unreachable!("insert builder engine changed the expression kind"),
        };
        self
    }

    /// Set the target column names for the INSERT statement.
    pub fn columns<I, S>(self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let columns = columns
            .into_iter()
            .map(|c| builder_identifier(c.as_ref()))
            .collect();
        self.edit(|expression| {
            engine::set_insert_columns(expression, columns)
                .expect("insert builder accepts target columns")
        })
    }

    /// Append a row of values to the VALUES clause.
    ///
    /// Call this method multiple times to insert multiple rows in a single statement.
    pub fn values<I>(self, values: I) -> Self
    where
        I: IntoIterator<Item = Expr>,
    {
        let row = values.into_iter().map(|v| v.0).collect();
        self.edit(|expression| {
            engine::apply_insert_values(expression, vec![row], true)
                .expect("insert builder accepts VALUES clauses")
        })
    }

    /// Set the source query for an `INSERT INTO ... SELECT ...` statement.
    ///
    /// When a query is set, the VALUES clause is ignored during generation.
    pub fn query(self, query: SelectBuilder) -> Self {
        self.edit(|expression| {
            engine::set_insert_query(expression, query.build())
                .expect("insert builder accepts query sources")
        })
    }

    /// Consume this builder and produce the final [`Expression::Insert`] AST node.
    pub fn build(self) -> Expression {
        Expression::Insert(Box::new(self.insert))
    }

    /// Consume this builder, generate, and return the SQL string.
    pub fn to_sql(self) -> String {
        generate_builder_sql(&self.build())
    }
}

// ---------------------------------------------------------------------------
// UpdateBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for constructing `UPDATE` statements.
///
/// Created by the [`update()`] entry-point function. Supports column assignments via
/// [`.set()`](UpdateBuilder::set), an optional WHERE predicate, and an optional
/// FROM clause for PostgreSQL/Snowflake-style multi-table updates.
pub struct UpdateBuilder {
    update: Update,
}

impl UpdateBuilder {
    fn edit(mut self, edit: impl FnOnce(&mut Expression)) -> Self {
        let mut expression = Expression::Update(Box::new(self.update));
        edit(&mut expression);
        self.update = match expression {
            Expression::Update(update) => *update,
            _ => unreachable!("update builder engine changed the expression kind"),
        };
        self
    }

    /// Add a `SET column = value` assignment.
    ///
    /// Call this method multiple times to set multiple columns.
    pub fn set(self, column: &str, value: Expr) -> Self {
        self.edit(|expression| {
            engine::append_update_assignments(
                expression,
                vec![(builder_identifier(column), value.0)],
            )
            .expect("update builder accepts SET assignments")
        })
    }

    /// Add a predicate to the WHERE clause, combining repeated calls with `AND`.
    pub fn where_(self, condition: Expr) -> Self {
        self.where_with_options(condition, ClauseOptions::default())
    }

    /// Add or replace the WHERE clause according to `options.append`.
    pub fn where_with_options(mut self, condition: Expr, options: ClauseOptions) -> Self {
        let mut expression = Expression::Update(Box::new(self.update));
        engine::apply_where(&mut expression, condition.0, options.append)
            .expect("update builder accepts WHERE clauses");
        self.update = match expression {
            Expression::Update(update) => *update,
            _ => unreachable!("update builder engine changed the expression kind"),
        };
        self
    }

    /// Set the FROM clause for PostgreSQL/Snowflake-style `UPDATE ... FROM ...` syntax.
    ///
    /// This allows joining against other tables within the UPDATE statement.
    pub fn from(self, table_name: &str) -> Self {
        self.edit(|expression| {
            engine::set_from(
                expression,
                vec![Expression::Table(Box::new(builder_table_ref(table_name)))],
            )
            .expect("update builder accepts FROM clauses")
        })
    }

    /// Consume this builder and produce the final [`Expression::Update`] AST node.
    pub fn build(self) -> Expression {
        Expression::Update(Box::new(self.update))
    }

    /// Consume this builder, generate, and return the SQL string.
    pub fn to_sql(self) -> String {
        generate_builder_sql(&self.build())
    }
}

// ---------------------------------------------------------------------------
// CaseBuilder
// ---------------------------------------------------------------------------

/// Start building a searched CASE expression (`CASE WHEN cond THEN result ... END`).
///
/// A searched CASE evaluates each WHEN condition independently. Use [`case_of()`] for
/// a simple CASE that compares an operand against values.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let expr = case()
///     .when(col("x").gt(lit(0)), lit("positive"))
///     .when(col("x").eq(lit(0)), lit("zero"))
///     .else_(lit("negative"))
///     .build();
/// assert_eq!(
///     expr.to_sql(),
///     "CASE WHEN x > 0 THEN 'positive' WHEN x = 0 THEN 'zero' ELSE 'negative' END"
/// );
/// ```
pub fn case() -> CaseBuilder {
    CaseBuilder {
        operand: None,
        whens: Vec::new(),
        else_: None,
    }
}

/// Start building a simple CASE expression (`CASE operand WHEN value THEN result ... END`).
///
/// A simple CASE compares the `operand` against each WHEN value for equality. Use
/// [`case()`] for a searched CASE with arbitrary boolean conditions.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let expr = case_of(col("status"))
///     .when(lit(1), lit("active"))
///     .when(lit(0), lit("inactive"))
///     .else_(lit("unknown"))
///     .build();
/// assert_eq!(
///     expr.to_sql(),
///     "CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' ELSE 'unknown' END"
/// );
/// ```
pub fn case_of(operand: Expr) -> CaseBuilder {
    CaseBuilder {
        operand: Some(operand.0),
        whens: Vec::new(),
        else_: None,
    }
}

/// Fluent builder for SQL `CASE` expressions (both searched and simple forms).
///
/// Created by [`case()`] (searched form) or [`case_of()`] (simple form). Add branches
/// with [`.when()`](CaseBuilder::when) and an optional default with
/// [`.else_()`](CaseBuilder::else_). Finalize with [`.build()`](CaseBuilder::build) to
/// get an [`Expr`], or [`.build_expr()`](CaseBuilder::build_expr) for a raw
/// [`Expression`].
pub struct CaseBuilder {
    operand: Option<Expression>,
    whens: Vec<(Expression, Expression)>,
    else_: Option<Expression>,
}

impl CaseBuilder {
    /// Add a `WHEN condition THEN result` branch to the CASE expression.
    ///
    /// For a searched CASE ([`case()`]), `condition` is a boolean predicate. For a simple
    /// CASE ([`case_of()`]), `condition` is the value to compare against the operand.
    pub fn when(mut self, condition: Expr, result: Expr) -> Self {
        self.whens.push((condition.0, result.0));
        self
    }

    /// Set the `ELSE result` default branch of the CASE expression.
    ///
    /// If not called, the CASE expression has no ELSE clause (implicitly NULL when
    /// no WHEN matches).
    pub fn else_(mut self, result: Expr) -> Self {
        self.else_ = Some(result.0);
        self
    }

    /// Consume this builder and produce an [`Expr`] wrapping the CASE expression.
    pub fn build(self) -> Expr {
        Expr(self.build_expr())
    }

    /// Consume this builder and produce the raw [`Expression::Case`] AST node.
    ///
    /// Use this instead of [`.build()`](CaseBuilder::build) when you need the
    /// [`Expression`] directly rather than an [`Expr`] wrapper.
    pub fn build_expr(self) -> Expression {
        let mut expression = engine::case(self.operand);
        for (condition, result) in self.whens {
            engine::append_case_when(&mut expression, condition, result)
                .expect("case builder accepts WHEN branches");
        }
        if let Some(result) = self.else_ {
            engine::set_case_else(&mut expression, result)
                .expect("case builder accepts ELSE branches");
        }
        expression
    }
}

// ---------------------------------------------------------------------------
// Subquery builders
// ---------------------------------------------------------------------------

/// Wrap a [`SelectBuilder`] as a named subquery for use in FROM or JOIN clauses.
///
/// The resulting [`Expr`] can be passed to [`SelectBuilder::from_expr()`] or used
/// in a join condition.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let inner = select(["id", "name"]).from("users").where_(col("active").eq(boolean(true)));
/// let sql = select(["sub.id"])
///     .from_expr(subquery(inner, "sub"))
///     .to_sql();
/// assert_eq!(
///     sql,
///     "SELECT sub.id FROM (SELECT id, name FROM users WHERE active = TRUE) AS sub"
/// );
/// ```
pub fn subquery(query: SelectBuilder, alias_name: &str) -> Expr {
    subquery_expr(query.build(), alias_name)
}

/// Wrap an existing [`Expression`] as a named subquery.
///
/// This is the lower-level version of [`subquery()`] that accepts a pre-built
/// [`Expression`] instead of a [`SelectBuilder`].
pub fn subquery_expr(expr: Expression, alias_name: &str) -> Expr {
    Expr(
        engine::subquery(expr, Some(builder_identifier(alias_name)), true)
            .expect("subquery builder source is a query"),
    )
}

// ---------------------------------------------------------------------------
// SetOpBuilder
// ---------------------------------------------------------------------------

/// Internal enum distinguishing the three kinds of set operations.
#[derive(Debug, Clone, Copy)]
enum SetOpKind {
    Union,
    Intersect,
    Except,
}

/// Fluent builder for `UNION`, `INTERSECT`, and `EXCEPT` set operations.
///
/// Created by the free functions [`union()`], [`union_all()`], [`intersect()`],
/// [`intersect_all()`], [`except_()`], [`except_all()`], or the corresponding methods
/// on [`SelectBuilder`]. Supports optional `.order_by()`, `.limit()`, and `.offset()`
/// clauses applied to the combined result.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = union_all(
///     select(["id"]).from("a"),
///     select(["id"]).from("b"),
/// )
/// .order_by(["id"])
/// .limit(10)
/// .to_sql();
/// ```
pub struct SetOpBuilder {
    kind: SetOpKind,
    left: Expression,
    right: Expression,
    all: bool,
    order_by: Option<OrderBy>,
    limit: Option<Box<Expression>>,
    offset: Option<Box<Expression>>,
}

impl SetOpBuilder {
    fn new(kind: SetOpKind, left: SelectBuilder, right: SelectBuilder, all: bool) -> Self {
        SetOpBuilder {
            kind,
            left: left.build(),
            right: right.build(),
            all,
            order_by: None,
            limit: None,
            offset: None,
        }
    }

    /// Add an ORDER BY clause applied to the combined set operation result.
    ///
    /// Expressions not already wrapped with [`.asc()`](Expr::asc) or
    /// [`.desc()`](Expr::desc) default to ascending order.
    pub fn order_by<I, E>(self, expressions: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        self.order_by_with_options(expressions, ClauseOptions::default())
    }

    /// Add or replace ordering expressions according to `options.append`.
    pub fn order_by_with_options<I, E>(mut self, expressions: I, options: ClauseOptions) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        let values: Vec<_> = expressions
            .into_iter()
            .map(|expression| engine::ordered(expression.into_expr().0))
            .collect();
        if options.append {
            self.order_by
                .get_or_insert_with(|| OrderBy {
                    siblings: false,
                    comments: Vec::new(),
                    expressions: Vec::new(),
                })
                .expressions
                .extend(values);
        } else {
            self.order_by = Some(OrderBy {
                siblings: false,
                comments: Vec::new(),
                expressions: values,
            });
        }
        self
    }

    /// Restrict the combined set operation result to `count` rows.
    pub fn limit(mut self, count: usize) -> Self {
        self.limit = Some(Box::new(Expression::Literal(Box::new(Literal::Number(
            count.to_string(),
        )))));
        self
    }

    /// Skip the first `count` rows from the combined set operation result.
    pub fn offset(mut self, count: usize) -> Self {
        self.offset = Some(Box::new(Expression::Literal(Box::new(Literal::Number(
            count.to_string(),
        )))));
        self
    }

    /// Consume this builder and produce the final set operation [`Expression`] AST node.
    ///
    /// The returned expression is one of [`Expression::Union`], [`Expression::Intersect`],
    /// or [`Expression::Except`] depending on how the builder was created.
    pub fn build(self) -> Expression {
        let kind = match self.kind {
            SetOpKind::Union => engine::SetKind::Union,
            SetOpKind::Intersect => engine::SetKind::Intersect,
            SetOpKind::Except => engine::SetKind::Except,
        };
        let mut expression = engine::set_operation(kind, self.left, self.right, !self.all)
            .expect("set builder operands are queries");
        if let Some(order_by) = self.order_by {
            engine::apply_order_by(&mut expression, order_by.expressions, false)
                .expect("set operations accept ORDER BY clauses");
        }
        if let Some(limit) = self.limit {
            engine::apply_limit(&mut expression, *limit)
                .expect("set operations accept LIMIT clauses");
        }
        if let Some(offset) = self.offset {
            engine::apply_offset(&mut expression, *offset)
                .expect("set operations accept OFFSET clauses");
        }
        expression
    }

    /// Consume this builder, generate, and return the SQL string.
    pub fn to_sql(self) -> String {
        generate_builder_sql(&self.build())
    }
}

/// Create a `UNION` (duplicate elimination) of two SELECT queries.
///
/// Returns a [`SetOpBuilder`] for optional ORDER BY / LIMIT / OFFSET chaining.
pub fn union(left: SelectBuilder, right: SelectBuilder) -> SetOpBuilder {
    SetOpBuilder::new(SetOpKind::Union, left, right, false)
}

/// Create a `UNION ALL` (keep duplicates) of two SELECT queries.
///
/// Returns a [`SetOpBuilder`] for optional ORDER BY / LIMIT / OFFSET chaining.
pub fn union_all(left: SelectBuilder, right: SelectBuilder) -> SetOpBuilder {
    SetOpBuilder::new(SetOpKind::Union, left, right, true)
}

/// Create an `INTERSECT` (rows common to both) of two SELECT queries.
///
/// Returns a [`SetOpBuilder`] for optional ORDER BY / LIMIT / OFFSET chaining.
pub fn intersect(left: SelectBuilder, right: SelectBuilder) -> SetOpBuilder {
    SetOpBuilder::new(SetOpKind::Intersect, left, right, false)
}

/// Create an `INTERSECT ALL` (keep duplicate common rows) of two SELECT queries.
///
/// Returns a [`SetOpBuilder`] for optional ORDER BY / LIMIT / OFFSET chaining.
pub fn intersect_all(left: SelectBuilder, right: SelectBuilder) -> SetOpBuilder {
    SetOpBuilder::new(SetOpKind::Intersect, left, right, true)
}

/// Create an `EXCEPT` (rows in left but not right) of two SELECT queries.
///
/// Returns a [`SetOpBuilder`] for optional ORDER BY / LIMIT / OFFSET chaining.
pub fn except_(left: SelectBuilder, right: SelectBuilder) -> SetOpBuilder {
    SetOpBuilder::new(SetOpKind::Except, left, right, false)
}

/// Create an `EXCEPT ALL` (keep duplicate difference rows) of two SELECT queries.
///
/// Returns a [`SetOpBuilder`] for optional ORDER BY / LIMIT / OFFSET chaining.
pub fn except_all(left: SelectBuilder, right: SelectBuilder) -> SetOpBuilder {
    SetOpBuilder::new(SetOpKind::Except, left, right, true)
}

// ---------------------------------------------------------------------------
// WindowDefBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing named `WINDOW` clause definitions.
///
/// Used with [`SelectBuilder::window()`] to define reusable window specifications.
/// Supports PARTITION BY and ORDER BY clauses.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = select(["id"])
///     .from("t")
///     .window(
///         "w",
///         WindowDefBuilder::new()
///             .partition_by(["dept"])
///             .order_by([col("salary").desc()]),
///     )
///     .to_sql();
/// ```
pub struct WindowDefBuilder {
    partition_by: Vec<Expression>,
    order_by: Vec<Ordered>,
}

impl WindowDefBuilder {
    /// Create a new, empty window definition builder with no partitioning or ordering.
    pub fn new() -> Self {
        WindowDefBuilder {
            partition_by: Vec::new(),
            order_by: Vec::new(),
        }
    }

    /// Set the PARTITION BY expressions for the window definition.
    pub fn partition_by<I, E>(mut self, expressions: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        self.partition_by = expressions.into_iter().map(|e| e.into_expr().0).collect();
        self
    }

    /// Set the ORDER BY expressions for the window definition.
    ///
    /// Expressions not already wrapped with [`.asc()`](Expr::asc) or
    /// [`.desc()`](Expr::desc) default to ascending order.
    pub fn order_by<I, E>(mut self, expressions: I) -> Self
    where
        I: IntoIterator<Item = E>,
        E: IntoExpr,
    {
        self.order_by = expressions
            .into_iter()
            .map(|e| {
                let expr = e.into_expr().0;
                match expr {
                    Expression::Ordered(o) => *o,
                    other => Ordered {
                        nulls_auto: false,
                        this: other,
                        desc: false,
                        nulls_first: None,
                        explicit_asc: false,
                        with_fill: None,
                    },
                }
            })
            .collect();
        self
    }
}

// ---------------------------------------------------------------------------
// Trait: IntoExpr
// ---------------------------------------------------------------------------

/// Conversion trait for types that can be turned into an [`Expr`].
///
/// This trait is implemented for:
///
/// - [`Expr`] -- returned as-is.
/// - `&str` and `String` -- converted to a column reference via [`col()`].
/// - [`Expression`] -- wrapped directly in an [`Expr`].
///
/// Note: `&str`/`String` inputs are treated as identifiers, not SQL string
/// literals. Use [`lit()`] for literal values.
///
/// It is used as a generic bound throughout the builder API so that functions like
/// [`select()`], [`SelectBuilder::order_by()`], and [`SelectBuilder::group_by()`] can
/// accept plain strings, [`Expr`] values, or raw [`Expression`] nodes interchangeably.
pub trait IntoExpr {
    /// Convert this value into an [`Expr`].
    fn into_expr(self) -> Expr;
}

impl IntoExpr for Expr {
    fn into_expr(self) -> Expr {
        self
    }
}

impl IntoExpr for &str {
    /// Convert a string slice to a column reference via [`col()`].
    fn into_expr(self) -> Expr {
        col(self)
    }
}

impl IntoExpr for String {
    /// Convert an owned string to a column reference via [`col()`].
    fn into_expr(self) -> Expr {
        col(&self)
    }
}

impl IntoExpr for Expression {
    /// Wrap a raw [`Expression`] in an [`Expr`].
    fn into_expr(self) -> Expr {
        Expr(self)
    }
}

// ---------------------------------------------------------------------------
// Trait: IntoLiteral
// ---------------------------------------------------------------------------

/// Conversion trait for types that can be turned into a SQL literal [`Expr`].
///
/// This trait is used by [`lit()`] to accept various Rust primitive types and convert
/// them into the appropriate SQL literal representation.
///
/// Implemented for:
///
/// - `&str`, `String` -- produce a SQL string literal (e.g. `'hello'`).
/// - `i32`, `i64`, `usize`, `f64` -- produce a SQL numeric literal (e.g. `42`, `3.14`).
/// - `bool` -- produce a SQL boolean literal (`TRUE` or `FALSE`).
pub trait IntoLiteral {
    /// Convert this value into a literal [`Expr`].
    fn into_literal(self) -> Expr;
}

impl IntoLiteral for &str {
    /// Produce a SQL string literal (e.g. `'hello'`).
    fn into_literal(self) -> Expr {
        Expr(Expression::Literal(Box::new(Literal::String(
            self.to_string(),
        ))))
    }
}

impl IntoLiteral for String {
    /// Produce a SQL string literal from an owned string.
    fn into_literal(self) -> Expr {
        Expr(Expression::Literal(Box::new(Literal::String(self))))
    }
}

impl IntoLiteral for i64 {
    /// Produce a SQL numeric literal from a 64-bit integer.
    fn into_literal(self) -> Expr {
        Expr(Expression::Literal(Box::new(Literal::Number(
            self.to_string(),
        ))))
    }
}

impl IntoLiteral for i32 {
    /// Produce a SQL numeric literal from a 32-bit integer.
    fn into_literal(self) -> Expr {
        Expr(Expression::Literal(Box::new(Literal::Number(
            self.to_string(),
        ))))
    }
}

impl IntoLiteral for usize {
    /// Produce a SQL numeric literal from a `usize`.
    fn into_literal(self) -> Expr {
        Expr(Expression::Literal(Box::new(Literal::Number(
            self.to_string(),
        ))))
    }
}

impl IntoLiteral for f64 {
    /// Produce a SQL numeric literal from a 64-bit float.
    fn into_literal(self) -> Expr {
        Expr(Expression::Literal(Box::new(Literal::Number(
            self.to_string(),
        ))))
    }
}

impl IntoLiteral for bool {
    /// Produce a SQL boolean literal (`TRUE` or `FALSE`).
    fn into_literal(self) -> Expr {
        Expr(Expression::Boolean(BooleanLiteral { value: self }))
    }
}

// ---------------------------------------------------------------------------
// MergeBuilder
// ---------------------------------------------------------------------------

/// Start building a `MERGE INTO` statement targeting the given table.
///
/// Returns a [`MergeBuilder`] which supports `.using()`, `.when_matched_update()`,
/// `.when_matched_delete()`, and `.when_not_matched_insert()`.
///
/// # Examples
///
/// ```
/// use polyglot_sql::builder::*;
///
/// let sql = merge_into("target")
///     .using("source", col("target.id").eq(col("source.id")))
///     .when_matched_update(vec![("name", col("source.name"))])
///     .when_not_matched_insert(&["id", "name"], vec![col("source.id"), col("source.name")])
///     .to_sql();
/// assert!(sql.contains("MERGE INTO"));
/// ```
pub fn merge_into(target: &str) -> MergeBuilder {
    MergeBuilder {
        expression: engine::merge(Expression::Table(Box::new(builder_table_ref(target)))),
    }
}

/// Fluent builder for constructing `MERGE INTO` statements.
///
/// Created by the [`merge_into()`] entry-point function.
pub struct MergeBuilder {
    expression: Expression,
}

impl MergeBuilder {
    /// Set the source table and ON join condition.
    pub fn using(mut self, source: &str, on: Expr) -> Self {
        engine::set_merge_using(
            &mut self.expression,
            Expression::Table(Box::new(builder_table_ref(source))),
            on.0,
        )
        .expect("merge builder accepts a USING clause");
        self
    }

    /// Add a `WHEN MATCHED THEN UPDATE SET` clause.
    pub fn when_matched_update(mut self, assignments: Vec<(&str, Expr)>) -> Self {
        let assignments = assignments
            .into_iter()
            .map(|(column, value)| (builder_identifier(column), value.0))
            .collect();
        engine::append_merge_update(&mut self.expression, assignments, None)
            .expect("merge builder accepts matched update actions");
        self
    }

    /// Add a `WHEN MATCHED THEN UPDATE SET` clause with an additional condition.
    pub fn when_matched_update_where(
        mut self,
        condition: Expr,
        assignments: Vec<(&str, Expr)>,
    ) -> Self {
        let assignments = assignments
            .into_iter()
            .map(|(column, value)| (builder_identifier(column), value.0))
            .collect();
        engine::append_merge_update(&mut self.expression, assignments, Some(condition.0))
            .expect("merge builder accepts conditional matched update actions");
        self
    }

    /// Add a `WHEN MATCHED THEN DELETE` clause.
    pub fn when_matched_delete(mut self) -> Self {
        engine::append_merge_delete(&mut self.expression, None)
            .expect("merge builder accepts matched delete actions");
        self
    }

    /// Add a conditional `WHEN MATCHED THEN DELETE` clause.
    pub fn when_matched_delete_where(mut self, condition: Expr) -> Self {
        engine::append_merge_delete(&mut self.expression, Some(condition.0))
            .expect("merge builder accepts conditional matched delete actions");
        self
    }

    /// Add a `WHEN NOT MATCHED THEN INSERT (cols) VALUES (vals)` clause.
    pub fn when_not_matched_insert(mut self, columns: &[&str], values: Vec<Expr>) -> Self {
        engine::append_merge_insert(
            &mut self.expression,
            columns
                .iter()
                .map(|column| builder_identifier(column))
                .collect(),
            values.into_iter().map(|value| value.0).collect(),
            None,
        )
        .expect("merge builder accepts not-matched insert actions");
        self
    }

    /// Add a conditional `WHEN NOT MATCHED THEN INSERT` clause.
    pub fn when_not_matched_insert_where(
        mut self,
        condition: Expr,
        columns: &[&str],
        values: Vec<Expr>,
    ) -> Self {
        engine::append_merge_insert(
            &mut self.expression,
            columns
                .iter()
                .map(|column| builder_identifier(column))
                .collect(),
            values.into_iter().map(|value| value.0).collect(),
            Some(condition.0),
        )
        .expect("merge builder accepts conditional not-matched insert actions");
        self
    }

    /// Consume this builder and produce the final [`Expression::Merge`] AST node.
    pub fn build(self) -> Expression {
        self.expression
    }

    /// Consume this builder, generate, and return the SQL string.
    pub fn to_sql(self) -> String {
        generate_builder_sql(&self.build())
    }
}

fn parse_simple_data_type(name: &str) -> DataType {
    let upper = name.trim().to_uppercase();
    match upper.as_str() {
        "INT" | "INTEGER" => DataType::Int {
            length: None,
            integer_spelling: upper == "INTEGER",
        },
        "BIGINT" => DataType::BigInt { length: None },
        "SMALLINT" => DataType::SmallInt { length: None },
        "TINYINT" => DataType::TinyInt { length: None },
        "FLOAT" => DataType::Float {
            precision: None,
            scale: None,
            real_spelling: false,
        },
        "DOUBLE" => DataType::Double {
            precision: None,
            scale: None,
        },
        "BOOLEAN" | "BOOL" => DataType::Boolean,
        "TEXT" => DataType::Text,
        "DATE" => DataType::Date,
        "TIMESTAMP" => DataType::Timestamp {
            precision: None,
            timezone: false,
        },
        "VARCHAR" => DataType::VarChar {
            length: None,
            parenthesized_length: false,
        },
        "CHAR" => DataType::Char { length: None },
        _ => {
            // Try to parse as a full type via the parser for complex types
            if let Ok(ast) =
                crate::parser::Parser::parse_sql(&format!("SELECT CAST(x AS {})", name))
            {
                if let Expression::Select(s) = &ast[0] {
                    if let Some(Expression::Cast(c)) = s.expressions.first() {
                        return c.to.clone();
                    }
                }
            }
            // Fallback: treat as a custom type
            DataType::Custom {
                name: name.to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let sql = select(["id", "name"]).from("users").to_sql();
        assert_eq!(sql, "SELECT id, name FROM users");
    }

    #[test]
    fn test_builder_quotes_unsafe_identifier_tokens() {
        let sql = select(["Name; DROP TABLE titanic"]).to_sql();
        assert_eq!(sql, r#"SELECT "Name; DROP TABLE titanic""#);
    }

    #[test]
    fn test_builder_string_literal_requires_lit() {
        let sql = select([lit("Name; DROP TABLE titanic")]).to_sql();
        assert_eq!(sql, "SELECT 'Name; DROP TABLE titanic'");
    }

    #[test]
    fn test_builder_quotes_unsafe_table_name_tokens() {
        let sql = select(["id"]).from("users; DROP TABLE x").to_sql();
        assert_eq!(sql, r#"SELECT id FROM "users; DROP TABLE x""#);
    }

    #[test]
    fn test_select_star() {
        let sql = select([star()]).from("users").to_sql();
        assert_eq!(sql, "SELECT * FROM users");
    }

    #[test]
    fn test_select_with_where() {
        let sql = select(["id", "name"])
            .from("users")
            .where_(col("age").gt(lit(18)))
            .to_sql();
        assert_eq!(sql, "SELECT id, name FROM users WHERE age > 18");
    }

    #[test]
    fn test_select_with_join() {
        let sql = select(["u.id", "o.amount"])
            .from("users")
            .join("orders", col("u.id").eq(col("o.user_id")))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT u.id, o.amount FROM users JOIN orders ON u.id = o.user_id"
        );
    }

    #[test]
    fn test_select_with_group_by_having() {
        let sql = select([col("dept"), func("COUNT", [star()]).alias("cnt")])
            .from("employees")
            .group_by(["dept"])
            .having(func("COUNT", [star()]).gt(lit(5)))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept HAVING COUNT(*) > 5"
        );
    }

    #[test]
    fn test_select_with_order_limit_offset() {
        let sql = select(["id", "name"])
            .from("users")
            .order_by(["name"])
            .limit(10)
            .offset(20)
            .to_sql();
        assert_eq!(
            sql,
            "SELECT id, name FROM users ORDER BY name LIMIT 10 OFFSET 20"
        );
    }

    #[test]
    fn test_select_distinct() {
        let sql = select(["name"]).from("users").distinct().to_sql();
        assert_eq!(sql, "SELECT DISTINCT name FROM users");
    }

    #[test]
    fn test_insert_values() {
        let sql = insert_into("users")
            .columns(["id", "name"])
            .values([lit(1), lit("Alice")])
            .values([lit(2), lit("Bob")])
            .to_sql();
        assert_eq!(
            sql,
            "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
        );
    }

    #[test]
    fn test_insert_select() {
        let sql = insert_into("archive")
            .columns(["id", "name"])
            .query(select(["id", "name"]).from("users"))
            .to_sql();
        assert_eq!(
            sql,
            "INSERT INTO archive (id, name) SELECT id, name FROM users"
        );
    }

    #[test]
    fn test_update() {
        let sql = update("users")
            .set("name", lit("Bob"))
            .set("age", lit(30))
            .where_(col("id").eq(lit(1)))
            .to_sql();
        assert_eq!(sql, "UPDATE users SET name = 'Bob', age = 30 WHERE id = 1");
    }

    #[test]
    fn test_delete() {
        let sql = delete("users").where_(col("id").eq(lit(1))).to_sql();
        assert_eq!(sql, "DELETE FROM users WHERE id = 1");
    }

    #[test]
    fn test_complex_where() {
        let sql = select(["id"])
            .from("users")
            .where_(
                col("age")
                    .gte(lit(18))
                    .and(col("active").eq(boolean(true)))
                    .and(col("name").like(lit("%test%"))),
            )
            .to_sql();
        assert_eq!(
            sql,
            "SELECT id FROM users WHERE age >= 18 AND active = TRUE AND name LIKE '%test%'"
        );
    }

    #[test]
    fn test_in_list() {
        let sql = select(["id"])
            .from("users")
            .where_(col("status").in_list([lit("active"), lit("pending")]))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT id FROM users WHERE status IN ('active', 'pending')"
        );
    }

    #[test]
    fn test_between() {
        let sql = select(["id"])
            .from("orders")
            .where_(col("amount").between(lit(100), lit(500)))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT id FROM orders WHERE amount BETWEEN 100 AND 500"
        );
    }

    #[test]
    fn test_is_null() {
        let sql = select(["id"])
            .from("users")
            .where_(col("email").is_null())
            .to_sql();
        assert_eq!(sql, "SELECT id FROM users WHERE email IS NULL");
    }

    #[test]
    fn test_arithmetic() {
        let sql = select([col("price").mul(col("quantity")).alias("total")])
            .from("items")
            .to_sql();
        assert_eq!(sql, "SELECT price * quantity AS total FROM items");
    }

    #[test]
    fn test_nested_operator_grouping() {
        assert_eq!(
            or(col("a").eq(lit(1)), col("b").eq(lit(2)))
                .and(col("c").eq(lit(3)))
                .to_sql(),
            "(a = 1 OR b = 2) AND c = 3"
        );
        assert_eq!(
            col("a")
                .eq(lit(1))
                .and(or(col("b").eq(lit(2)), col("c").eq(lit(3))))
                .to_sql(),
            "a = 1 AND (b = 2 OR c = 3)"
        );
        assert_eq!(col("a").add(col("b")).mul(col("c")).to_sql(), "(a + b) * c");
        assert_eq!(col("a").mul(col("b").add(col("c"))).to_sql(), "a * (b + c)");
        assert_eq!(col("a").sub(col("b").sub(col("c"))).to_sql(), "a - (b - c)");
        assert_eq!(col("a").div(col("b").div(col("c"))).to_sql(), "a / (b / c)");
    }

    #[test]
    fn test_cast() {
        let sql = select([col("id").cast("VARCHAR")]).from("users").to_sql();
        assert_eq!(sql, "SELECT CAST(id AS VARCHAR) FROM users");
    }

    #[test]
    fn test_cast_int128() {
        for name in ["HUGEINT", "INT128"] {
            let Expression::Cast(cast) = col("value").cast(name).0 else {
                panic!("expected cast");
            };
            assert_eq!(cast.to, DataType::Int128);
        }
    }

    #[test]
    fn test_cast_unsigned_integer_types() {
        for (name, expected) in [
            ("UTINYINT", DataType::UInt8),
            ("UINT8", DataType::UInt8),
            ("USMALLINT", DataType::UInt16),
            ("UINT16", DataType::UInt16),
            ("UINTEGER", DataType::UInt32),
            ("UINT32", DataType::UInt32),
            ("UBIGINT", DataType::UInt64),
            ("UINT64", DataType::UInt64),
            ("UHUGEINT", DataType::UInt128),
            ("UINT128", DataType::UInt128),
        ] {
            let Expression::Cast(cast) = col("value").cast(name).0 else {
                panic!("expected cast")
            };
            assert_eq!(cast.to, expected, "{name}");
        }
    }

    #[test]
    fn test_from_starter() {
        let sql = from("users").select_cols(["id", "name"]).to_sql();
        assert_eq!(sql, "SELECT id, name FROM users");
    }

    #[test]
    fn test_qualified_column() {
        let sql = select([col("u.id"), col("u.name")]).from("users").to_sql();
        assert_eq!(sql, "SELECT u.id, u.name FROM users");
    }

    #[test]
    fn test_nested_dot_column() {
        let sql = select([col("t.s.f")]).from("users").to_sql();
        assert_eq!(sql, "SELECT t.s.f FROM users");
    }

    #[test]
    fn test_not_condition() {
        let sql = select(["id"])
            .from("users")
            .where_(not(col("active").eq(boolean(true))))
            .to_sql();
        assert_eq!(sql, "SELECT id FROM users WHERE NOT active = TRUE");
    }

    #[test]
    fn test_order_by_desc() {
        let sql = select(["id", "name"])
            .from("users")
            .order_by([col("name").desc()])
            .to_sql();
        assert_eq!(sql, "SELECT id, name FROM users ORDER BY name DESC");
    }

    #[test]
    fn test_left_join() {
        let sql = select(["u.id", "o.amount"])
            .from("users")
            .left_join("orders", col("u.id").eq(col("o.user_id")))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT u.id, o.amount FROM users LEFT JOIN orders ON u.id = o.user_id"
        );
    }

    #[test]
    fn test_build_returns_expression() {
        let expr = select(["id"]).from("users").build();
        assert!(matches!(expr, Expression::Select(_)));
    }

    #[test]
    fn test_expr_interop() {
        // Can use Expr in select list
        let age_check = col("age").gt(lit(18));
        let sql = select([col("id"), age_check.alias("is_adult")])
            .from("users")
            .to_sql();
        assert_eq!(sql, "SELECT id, age > 18 AS is_adult FROM users");
    }

    // -- Step 2: sql_expr / condition tests --

    #[test]
    fn test_sql_expr_simple() {
        let expr = sql_expr("age > 18");
        let sql = select(["id"]).from("users").where_(expr).to_sql();
        assert_eq!(sql, "SELECT id FROM users WHERE age > 18");
    }

    #[test]
    fn test_sql_expr_compound() {
        let expr = sql_expr("a > 1 AND b < 10");
        let sql = select(["*"]).from("t").where_(expr).to_sql();
        assert_eq!(sql, "SELECT * FROM t WHERE a > 1 AND b < 10");
    }

    #[test]
    fn test_sql_expr_function() {
        let expr = sql_expr("COALESCE(a, b, 0)");
        let sql = select([expr.alias("val")]).from("t").to_sql();
        assert_eq!(sql, "SELECT COALESCE(a, b, 0) AS val FROM t");
    }

    #[test]
    fn test_condition_alias() {
        let cond = condition("x > 0");
        let sql = select(["*"]).from("t").where_(cond).to_sql();
        assert_eq!(sql, "SELECT * FROM t WHERE x > 0");
    }

    // -- Step 3: ilike, rlike, not_in tests --

    #[test]
    fn test_ilike() {
        let sql = select(["id"])
            .from("users")
            .where_(col("name").ilike(lit("%test%")))
            .to_sql();
        assert_eq!(sql, "SELECT id FROM users WHERE name ILIKE '%test%'");
    }

    #[test]
    fn test_rlike() {
        let sql = select(["id"])
            .from("users")
            .where_(col("name").rlike(lit("^[A-Z]")))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT id FROM users WHERE REGEXP_LIKE(name, '^[A-Z]')"
        );
    }

    #[test]
    fn test_not_in() {
        let sql = select(["id"])
            .from("users")
            .where_(col("status").not_in([lit("deleted"), lit("banned")]))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT id FROM users WHERE status NOT IN ('deleted', 'banned')"
        );
    }

    #[test]
    fn repeated_clauses_append_unless_replacement_is_requested() {
        let appended = select(["x"])
            .where_(col("x").gt(lit(0)))
            .where_(col("x").lt(lit(10)))
            .group_by(["x"])
            .group_by(["y"])
            .to_sql();
        assert_eq!(appended, "SELECT x WHERE x > 0 AND x < 10 GROUP BY x, y");

        let replaced = select(["x"])
            .where_(col("x").gt(lit(0)))
            .where_with_options(col("x").eq(lit(5)), ClauseOptions { append: false })
            .group_by(["x"])
            .group_by_with_options(["y"], ClauseOptions { append: false })
            .to_sql();
        assert_eq!(replaced, "SELECT x WHERE x = 5 GROUP BY y");
    }

    // -- Step 4: CaseBuilder tests --

    #[test]
    fn test_case_searched() {
        let expr = case()
            .when(col("x").gt(lit(0)), lit("positive"))
            .when(col("x").eq(lit(0)), lit("zero"))
            .else_(lit("negative"))
            .build();
        let sql = select([expr.alias("label")]).from("t").to_sql();
        assert_eq!(
            sql,
            "SELECT CASE WHEN x > 0 THEN 'positive' WHEN x = 0 THEN 'zero' ELSE 'negative' END AS label FROM t"
        );
    }

    #[test]
    fn test_case_simple() {
        let expr = case_of(col("status"))
            .when(lit(1), lit("active"))
            .when(lit(0), lit("inactive"))
            .build();
        let sql = select([expr.alias("status_label")]).from("t").to_sql();
        assert_eq!(
            sql,
            "SELECT CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END AS status_label FROM t"
        );
    }

    #[test]
    fn test_case_no_else() {
        let expr = case().when(col("x").gt(lit(0)), lit("yes")).build();
        let sql = select([expr]).from("t").to_sql();
        assert_eq!(sql, "SELECT CASE WHEN x > 0 THEN 'yes' END FROM t");
    }

    // -- Step 5: subquery tests --

    #[test]
    fn test_subquery_in_from() {
        let inner = select(["id", "name"])
            .from("users")
            .where_(col("active").eq(boolean(true)));
        let outer = select(["sub.id"])
            .from_expr(subquery(inner, "sub"))
            .to_sql();
        assert_eq!(
            outer,
            "SELECT sub.id FROM (SELECT id, name FROM users WHERE active = TRUE) AS sub"
        );
    }

    #[test]
    fn test_subquery_in_join() {
        let inner = select([col("user_id"), func("SUM", [col("amount")]).alias("total")])
            .from("orders")
            .group_by(["user_id"]);
        let sql = select(["u.name", "o.total"])
            .from("users")
            .join("orders", col("u.id").eq(col("o.user_id")))
            .to_sql();
        assert!(sql.contains("JOIN"));
        // Just verify the subquery builder doesn't panic
        let _sub = subquery(inner, "o");
    }

    // -- Step 6: SetOpBuilder tests --

    #[test]
    fn test_union() {
        let sql = union(select(["id"]).from("a"), select(["id"]).from("b")).to_sql();
        assert_eq!(sql, "SELECT id FROM a UNION SELECT id FROM b");
    }

    #[test]
    fn test_union_all() {
        let sql = union_all(select(["id"]).from("a"), select(["id"]).from("b")).to_sql();
        assert_eq!(sql, "SELECT id FROM a UNION ALL SELECT id FROM b");
    }

    #[test]
    fn test_intersect_builder() {
        let sql = intersect(select(["id"]).from("a"), select(["id"]).from("b")).to_sql();
        assert_eq!(sql, "SELECT id FROM a INTERSECT SELECT id FROM b");
    }

    #[test]
    fn test_except_builder() {
        let sql = except_(select(["id"]).from("a"), select(["id"]).from("b")).to_sql();
        assert_eq!(sql, "SELECT id FROM a EXCEPT SELECT id FROM b");
    }

    #[test]
    fn test_union_with_order_limit() {
        let sql = union(select(["id"]).from("a"), select(["id"]).from("b"))
            .order_by(["id"])
            .limit(10)
            .to_sql();
        assert!(sql.contains("UNION"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT"));
    }

    #[test]
    fn test_select_builder_union() {
        let sql = select(["id"])
            .from("a")
            .union(select(["id"]).from("b"))
            .to_sql();
        assert_eq!(sql, "SELECT id FROM a UNION SELECT id FROM b");
    }

    // -- Step 7: SelectBuilder extensions tests --

    #[test]
    fn test_qualify() {
        let sql = select(["id", "name"])
            .from("users")
            .qualify(col("rn").eq(lit(1)))
            .to_sql();
        assert_eq!(sql, "SELECT id, name FROM users QUALIFY rn = 1");
    }

    #[test]
    fn test_right_join() {
        let sql = select(["u.id", "o.amount"])
            .from("users")
            .right_join("orders", col("u.id").eq(col("o.user_id")))
            .to_sql();
        assert_eq!(
            sql,
            "SELECT u.id, o.amount FROM users RIGHT JOIN orders ON u.id = o.user_id"
        );
    }

    #[test]
    fn test_cross_join() {
        let sql = select(["a.x", "b.y"]).from("a").cross_join("b").to_sql();
        assert_eq!(sql, "SELECT a.x, b.y FROM a CROSS JOIN b");
    }

    #[test]
    fn test_lateral_view() {
        let sql = select(["id", "col_val"])
            .from("t")
            .lateral_view(func("EXPLODE", [col("arr")]), "lv", ["col_val"])
            .to_sql();
        assert!(sql.contains("LATERAL VIEW"));
        assert!(sql.contains("EXPLODE"));

        let expression = select(["id"])
            .from("t")
            .lateral_view_with_options(
                func("EXPLODE", [col("arr")]),
                "lv",
                ["value"],
                LateralViewOptions { outer: true },
            )
            .build();
        let Expression::Select(select) = expression else {
            panic!("expected SELECT")
        };
        assert!(select.lateral_views[0].outer);
    }

    #[test]
    fn test_window_clause() {
        let sql = select(["id"])
            .from("t")
            .window(
                "w",
                WindowDefBuilder::new()
                    .partition_by(["dept"])
                    .order_by(["salary"]),
            )
            .to_sql();
        assert!(sql.contains("WINDOW"));
        assert!(sql.contains("PARTITION BY"));
    }

    // -- XOR operator tests --

    #[test]
    fn test_xor() {
        let sql = select(["*"])
            .from("t")
            .where_(col("a").xor(col("b")))
            .to_sql();
        assert_eq!(sql, "SELECT * FROM t WHERE a XOR b");
    }

    // -- FOR UPDATE / FOR SHARE tests --

    #[test]
    fn test_for_update() {
        let sql = select(["id"]).from("t").for_update().to_sql();
        assert_eq!(sql, "SELECT id FROM t FOR UPDATE");
    }

    #[test]
    fn test_for_share() {
        let sql = select(["id"]).from("t").for_share().to_sql();
        assert_eq!(sql, "SELECT id FROM t FOR SHARE");
    }

    // -- Hint tests --

    #[test]
    fn test_hint() {
        let sql = select(["*"]).from("t").hint("FULL(t)").to_sql();
        assert!(sql.contains("FULL(t)"), "Expected hint in: {}", sql);
    }

    // -- CTAS tests --

    #[test]
    fn test_ctas() {
        let expr = select(["*"]).from("t").ctas("new_table");
        let sql = Generator::sql(&expr).unwrap();
        assert_eq!(sql, "CREATE TABLE new_table AS SELECT * FROM t");

        let expr = select(["*"]).from("t").ctas_with_options(
            "new_table",
            CtasOptions {
                replace: true,
                temporary: true,
            },
        );
        let Expression::CreateTable(create) = expr else {
            panic!("expected CREATE TABLE")
        };
        assert!(create.or_replace);
        assert!(create.temporary);
    }

    // -- MergeBuilder tests --

    #[test]
    fn test_merge_update_insert() {
        let sql = merge_into("target")
            .using("source", col("target.id").eq(col("source.id")))
            .when_matched_update(vec![("name", col("source.name"))])
            .when_not_matched_insert(&["id", "name"], vec![col("source.id"), col("source.name")])
            .to_sql();
        assert!(
            sql.contains("MERGE INTO"),
            "Expected MERGE INTO in: {}",
            sql
        );
        assert!(sql.contains("USING"), "Expected USING in: {}", sql);
        assert!(
            sql.contains("WHEN MATCHED"),
            "Expected WHEN MATCHED in: {}",
            sql
        );
        assert!(
            sql.contains("UPDATE SET"),
            "Expected UPDATE SET in: {}",
            sql
        );
        assert!(
            sql.contains("WHEN NOT MATCHED"),
            "Expected WHEN NOT MATCHED in: {}",
            sql
        );
        assert!(sql.contains("INSERT"), "Expected INSERT in: {}", sql);
    }

    #[test]
    fn test_merge_delete() {
        let sql = merge_into("target")
            .using("source", col("target.id").eq(col("source.id")))
            .when_matched_delete()
            .to_sql();
        assert!(
            sql.contains("MERGE INTO"),
            "Expected MERGE INTO in: {}",
            sql
        );
        assert!(
            sql.contains("WHEN MATCHED THEN DELETE"),
            "Expected WHEN MATCHED THEN DELETE in: {}",
            sql
        );
    }

    #[test]
    fn test_merge_with_condition() {
        let sql = merge_into("target")
            .using("source", col("target.id").eq(col("source.id")))
            .when_matched_update_where(
                col("source.active").eq(boolean(true)),
                vec![("name", col("source.name"))],
            )
            .to_sql();
        assert!(
            sql.contains("MERGE INTO"),
            "Expected MERGE INTO in: {}",
            sql
        );
        assert!(
            sql.contains("AND source.active = TRUE"),
            "Expected condition in: {}",
            sql
        );
    }
}
