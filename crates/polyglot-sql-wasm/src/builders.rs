//! WASM-compatible builder wrappers for programmatic SQL construction.
//!
//! These types wrap `polyglot_sql::builder` types with `#[wasm_bindgen]`-compatible
//! interfaces. The key difference from the core builder API is that WASM types use
//! `&self` (cloning internally) or `&mut self` (with `Option<Inner>` pattern) instead
//! of move semantics, since JavaScript cannot express Rust ownership.

use polyglot_sql::builder::{
    self as core_builder, CaseBuilder as CoreCaseBuilder, DeleteBuilder as CoreDeleteBuilder,
    Expr as CoreExpr, InsertBuilder as CoreInsertBuilder, MergeBuilder as CoreMergeBuilder,
    SelectBuilder as CoreSelectBuilder, SetOpBuilder as CoreSetOpBuilder,
    UpdateBuilder as CoreUpdateBuilder, WindowDefBuilder as CoreWindowDefBuilder,
};
use polyglot_sql::dialects::{Dialect, DialectType};
use polyglot_sql::expressions::Expression;
use polyglot_sql::generator::{Generator, GeneratorConfig, NotInStyle};
use wasm_bindgen::prelude::*;

// ---------------------------------------------------------------------------
// WasmExpr — wraps core::builder::Expr
// ---------------------------------------------------------------------------

/// A SQL expression handle. Methods use `&self` (cloning internally) so JS
/// can reuse references.
#[wasm_bindgen]
pub struct WasmExpr {
    inner: CoreExpr,
}

impl WasmExpr {
    fn new(inner: CoreExpr) -> Self {
        WasmExpr { inner }
    }

    pub(crate) fn inner(&self) -> &CoreExpr {
        &self.inner
    }
}

#[wasm_bindgen]
impl WasmExpr {
    // -- Comparison operators --

    /// Produce `self = other`.
    pub fn eq(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().eq(other.inner.clone()))
    }

    /// Produce `self <> other`.
    pub fn neq(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().neq(other.inner.clone()))
    }

    /// Produce `self < other`.
    pub fn lt(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().lt(other.inner.clone()))
    }

    /// Produce `self <= other`.
    pub fn lte(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().lte(other.inner.clone()))
    }

    /// Produce `self > other`.
    pub fn gt(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().gt(other.inner.clone()))
    }

    /// Produce `self >= other`.
    pub fn gte(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().gte(other.inner.clone()))
    }

    // -- Logical operators --

    /// Produce `self AND other`.
    #[wasm_bindgen(js_name = "and")]
    pub fn and_(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().and(other.inner.clone()))
    }

    /// Produce `self OR other`.
    #[wasm_bindgen(js_name = "or")]
    pub fn or_(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().or(other.inner.clone()))
    }

    /// Produce `NOT self`.
    pub fn not(&self) -> WasmExpr {
        WasmExpr::new(self.inner.clone().not())
    }

    /// Produce `self XOR other`.
    pub fn xor(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().xor(other.inner.clone()))
    }

    // -- Arithmetic operators --

    /// Produce `self + other`.
    pub fn add(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().add(other.inner.clone()))
    }

    /// Produce `self - other`.
    pub fn sub(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().sub(other.inner.clone()))
    }

    /// Produce `self * other`.
    pub fn mul(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().mul(other.inner.clone()))
    }

    /// Produce `self / other`.
    pub fn div(&self, other: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().div(other.inner.clone()))
    }

    // -- Pattern matching --

    /// Produce `self LIKE pattern`.
    pub fn like(&self, pattern: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().like(pattern.inner.clone()))
    }

    /// Produce `self ILIKE pattern`.
    pub fn ilike(&self, pattern: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().ilike(pattern.inner.clone()))
    }

    /// Produce `REGEXP_LIKE(self, pattern)`.
    pub fn rlike(&self, pattern: &WasmExpr) -> WasmExpr {
        WasmExpr::new(self.inner.clone().rlike(pattern.inner.clone()))
    }

    // -- Predicates --

    /// Produce `self IS NULL`.
    pub fn is_null(&self) -> WasmExpr {
        WasmExpr::new(self.inner.clone().is_null())
    }

    /// Produce `self IS NOT NULL`.
    pub fn is_not_null(&self) -> WasmExpr {
        WasmExpr::new(self.inner.clone().is_not_null())
    }

    /// Produce `self BETWEEN low AND high`.
    pub fn between(&self, low: &WasmExpr, high: &WasmExpr) -> WasmExpr {
        WasmExpr::new(
            self.inner
                .clone()
                .between(low.inner.clone(), high.inner.clone()),
        )
    }

    /// Produce `self IN (values...)`.
    pub fn in_list(&self, values: &WasmExprArray) -> WasmExpr {
        WasmExpr::new(self.inner.clone().in_list(values.inner.clone()))
    }

    /// Produce `self NOT IN (values...)`.
    pub fn not_in(&self, values: &WasmExprArray) -> WasmExpr {
        WasmExpr::new(self.inner.clone().not_in(values.inner.clone()))
    }

    // -- Transform --

    /// Produce `self AS name`.
    pub fn alias(&self, name: &str) -> WasmExpr {
        WasmExpr::new(self.inner.clone().alias(name))
    }

    /// Produce `CAST(self AS type)`.
    pub fn cast(&self, to: &str) -> WasmExpr {
        WasmExpr::new(self.inner.clone().cast(to))
    }

    /// Wrap with ascending sort order.
    pub fn asc(&self) -> WasmExpr {
        WasmExpr::new(self.inner.clone().asc())
    }

    /// Wrap with descending sort order.
    pub fn desc(&self) -> WasmExpr {
        WasmExpr::new(self.inner.clone().desc())
    }

    // -- Output --

    /// Generate SQL string (generic dialect).
    pub fn to_sql(&self) -> Result<String, JsValue> {
        let config = GeneratorConfig {
            not_in_style: NotInStyle::Infix,
            ..Default::default()
        };
        let mut generator = Generator::with_config(config);
        generator
            .generate(&self.inner.0)
            .map_err(|e| js_error(format!("Failed to generate SQL for expression: {e}")))
    }

    /// Return the expression AST as a JSON value.
    pub fn to_json(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(&self.inner.0).map_err(|e| JsValue::from_str(&e.to_string()))
    }
}

// ---------------------------------------------------------------------------
// WasmExprArray — helper for passing lists of expressions across WASM boundary
// ---------------------------------------------------------------------------

/// An array of expressions, used to pass lists across the WASM boundary.
#[wasm_bindgen]
pub struct WasmExprArray {
    inner: Vec<CoreExpr>,
}

#[wasm_bindgen]
impl WasmExprArray {
    /// Create an empty expression array.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        WasmExprArray { inner: Vec::new() }
    }

    /// Push an expression.
    pub fn push(&mut self, expr: &WasmExpr) {
        self.inner.push(expr.inner.clone());
    }

    /// Push a column reference by name.
    pub fn push_col(&mut self, name: &str) {
        self.inner.push(core_builder::col(name));
    }

    /// Push a string literal.
    pub fn push_str(&mut self, value: &str) {
        self.inner.push(core_builder::lit(value));
    }

    /// Push an integer literal.
    pub fn push_int(&mut self, value: i32) {
        self.inner.push(core_builder::lit(value));
    }

    /// Push a float literal.
    pub fn push_float(&mut self, value: f64) {
        self.inner.push(core_builder::lit(value));
    }

    /// Push a star (*) expression.
    pub fn push_star(&mut self) {
        self.inner.push(core_builder::star());
    }

    /// Get the number of expressions.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

// ---------------------------------------------------------------------------
// WasmAssignmentArray — helper for MERGE WHEN MATCHED UPDATE
// ---------------------------------------------------------------------------

/// An array of (column, value) assignments for UPDATE SET / MERGE clauses.
#[wasm_bindgen]
pub struct WasmAssignmentArray {
    inner: Vec<(String, CoreExpr)>,
}

#[wasm_bindgen]
impl WasmAssignmentArray {
    /// Create an empty assignment array.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        WasmAssignmentArray { inner: Vec::new() }
    }

    /// Add a column = value assignment.
    pub fn push(&mut self, column: &str, value: &WasmExpr) {
        self.inner.push((column.to_string(), value.inner.clone()));
    }

    /// Get the number of assignments.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

// ---------------------------------------------------------------------------
// Free functions — expression helpers
// ---------------------------------------------------------------------------

/// Create a column reference expression.
#[wasm_bindgen]
pub fn wasm_col(name: &str) -> WasmExpr {
    WasmExpr::new(core_builder::col(name))
}

/// Create a table reference expression.
#[wasm_bindgen]
pub fn wasm_table(name: &str) -> WasmExpr {
    WasmExpr::new(core_builder::table(name))
}

/// Create a star (*) expression.
#[wasm_bindgen]
pub fn wasm_star() -> WasmExpr {
    WasmExpr::new(core_builder::star())
}

/// Create a literal from a JS value (string, number, boolean, null).
#[wasm_bindgen]
pub fn wasm_lit(value: JsValue) -> WasmExpr {
    if value.is_null() || value.is_undefined() {
        return WasmExpr::new(core_builder::null());
    }
    if let Some(b) = value.as_bool() {
        return WasmExpr::new(core_builder::boolean(b));
    }
    if let Some(n) = value.as_f64() {
        // Check if it's an integer
        if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
            return WasmExpr::new(core_builder::lit(n as i64));
        }
        return WasmExpr::new(core_builder::lit(n));
    }
    if let Some(s) = value.as_string() {
        return WasmExpr::new(core_builder::lit(s));
    }
    // Fallback: treat as null
    WasmExpr::new(core_builder::null())
}

/// Create a SQL NULL expression.
#[wasm_bindgen]
pub fn wasm_null() -> WasmExpr {
    WasmExpr::new(core_builder::null())
}

/// Create a SQL boolean literal.
#[wasm_bindgen]
pub fn wasm_boolean(value: bool) -> WasmExpr {
    WasmExpr::new(core_builder::boolean(value))
}

/// Parse a raw SQL fragment into an expression.
#[wasm_bindgen]
pub fn wasm_sql_expr(sql: &str) -> WasmExpr {
    WasmExpr::new(core_builder::sql_expr(sql))
}

/// Create a function call expression.
#[wasm_bindgen]
pub fn wasm_func(name: &str, args: &WasmExprArray) -> WasmExpr {
    WasmExpr::new(core_builder::func(name, args.inner.clone()))
}

/// Combine two expressions with AND.
#[wasm_bindgen]
pub fn wasm_and(left: &WasmExpr, right: &WasmExpr) -> WasmExpr {
    WasmExpr::new(core_builder::and(left.inner.clone(), right.inner.clone()))
}

/// Combine two expressions with OR.
#[wasm_bindgen]
pub fn wasm_or(left: &WasmExpr, right: &WasmExpr) -> WasmExpr {
    WasmExpr::new(core_builder::or(left.inner.clone(), right.inner.clone()))
}

/// Negate an expression with NOT.
#[wasm_bindgen]
pub fn wasm_not(expr: &WasmExpr) -> WasmExpr {
    WasmExpr::new(core_builder::not(expr.inner.clone()))
}

/// Create a CAST(expr AS type) expression.
#[wasm_bindgen]
pub fn wasm_cast(expr: &WasmExpr, to: &str) -> WasmExpr {
    WasmExpr::new(core_builder::cast(expr.inner.clone(), to))
}

/// Create an expr AS name alias expression.
#[wasm_bindgen]
pub fn wasm_alias(expr: &WasmExpr, name: &str) -> WasmExpr {
    WasmExpr::new(core_builder::alias(expr.inner.clone(), name))
}

/// Wrap a SelectBuilder as a named subquery expression. Consumes the builder.
#[wasm_bindgen]
pub fn wasm_subquery(query: &mut WasmSelectBuilder, alias: &str) -> Result<WasmExpr, JsValue> {
    let q = take_owned(&mut query.inner, "SelectBuilder")?;
    Ok(WasmExpr::new(core_builder::subquery(q, alias)))
}

/// Create a `COUNT(DISTINCT expr)` expression.
#[wasm_bindgen]
pub fn wasm_count_distinct(expr: &WasmExpr) -> WasmExpr {
    WasmExpr::new(core_builder::count_distinct(expr.inner().clone()))
}

/// Create an `EXTRACT(field FROM expr)` expression.
#[wasm_bindgen]
pub fn wasm_extract(field: &str, expr: &WasmExpr) -> WasmExpr {
    WasmExpr::new(core_builder::extract_(field, expr.inner().clone()))
}

// ---------------------------------------------------------------------------
// Helper: generate SQL with a specific dialect
// ---------------------------------------------------------------------------

fn generate_sql(expr: &Expression, dialect_str: &str) -> Result<String, JsValue> {
    let dialect_type = dialect_str
        .parse::<DialectType>()
        .unwrap_or(DialectType::Generic);

    if dialect_type == DialectType::Generic {
        let config = GeneratorConfig {
            dialect: Some(DialectType::Generic),
            not_in_style: NotInStyle::Infix,
            ..Default::default()
        };
        let mut generator = Generator::with_config(config);
        return generator
            .generate(expr)
            .map_err(|e| js_error(format!("Failed to generate SQL for generic dialect: {e}")));
    }

    let d = Dialect::get(dialect_type);
    d.generate(expr).map_err(|e| {
        js_error(format!(
            "Failed to generate SQL for dialect '{dialect_str}': {e}"
        ))
    })
}

fn js_error(message: impl Into<String>) -> JsValue {
    #[cfg(target_arch = "wasm32")]
    {
        JsValue::from_str(&message.into())
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = message.into();
        JsValue::NULL
    }
}

fn take_owned<T>(slot: &mut Option<T>, name: &str) -> Result<T, JsValue> {
    slot.take()
        .ok_or_else(|| js_error(format!("{name} already consumed")))
}

fn build_json(expr: &Expression) -> Result<JsValue, JsValue> {
    serde_wasm_bindgen::to_value(expr).map_err(|e| JsValue::from_str(&e.to_string()))
}

// ---------------------------------------------------------------------------
// WasmSelectBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for SELECT statements.
///
/// Uses `Option<CoreSelectBuilder>` internally. Each method takes/puts the builder
/// using the `&mut self` pattern required by wasm_bindgen.
#[wasm_bindgen]
pub struct WasmSelectBuilder {
    inner: Option<CoreSelectBuilder>,
}

#[wasm_bindgen]
impl WasmSelectBuilder {
    /// Create a new, empty SELECT builder.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        WasmSelectBuilder {
            inner: Some(core_builder::select(std::iter::empty::<&str>())),
        }
    }

    // -- Column selection --

    /// Add a single expression to the SELECT list.
    pub fn select_expr(&mut self, expr: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.select_cols([expr.inner.clone()]));
        }
    }

    /// Add a column by name to the SELECT list.
    pub fn select_col(&mut self, name: &str) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.select_cols([name]));
        }
    }

    /// Add a star (*) to the SELECT list.
    pub fn select_star(&mut self) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.select_cols([core_builder::star()]));
        }
    }

    /// Add multiple expressions to the SELECT list.
    pub fn select_exprs(&mut self, exprs: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.select_cols(exprs.inner.clone()));
        }
    }

    // -- FROM --

    /// Set the FROM clause to a table by name.
    pub fn from(&mut self, table: &str) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.from(table));
        }
    }

    /// Set the FROM clause to an arbitrary expression.
    pub fn from_expr(&mut self, expr: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.from_expr(expr.inner.clone()));
        }
    }

    // -- JOINs --

    /// Add an INNER JOIN.
    pub fn join(&mut self, table: &str, on: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.join(table, on.inner.clone()));
        }
    }

    /// Add a LEFT JOIN.
    pub fn left_join(&mut self, table: &str, on: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.left_join(table, on.inner.clone()));
        }
    }

    /// Add a RIGHT JOIN.
    pub fn right_join(&mut self, table: &str, on: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.right_join(table, on.inner.clone()));
        }
    }

    /// Add a CROSS JOIN.
    pub fn cross_join(&mut self, table: &str) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.cross_join(table));
        }
    }

    // -- WHERE --

    /// Set the WHERE clause to a condition expression.
    pub fn where_expr(&mut self, condition: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.where_(condition.inner.clone()));
        }
    }

    /// Set the WHERE clause by parsing a raw SQL condition string.
    pub fn where_sql(&mut self, sql: &str) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.where_(core_builder::sql_expr(sql)));
        }
    }

    // -- GROUP BY, HAVING --

    /// Set the GROUP BY clause.
    pub fn group_by_cols(&mut self, cols: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.group_by(cols.inner.clone()));
        }
    }

    /// Set the HAVING clause.
    pub fn having(&mut self, condition: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.having(condition.inner.clone()));
        }
    }

    // -- ORDER BY --

    /// Set the ORDER BY clause.
    pub fn order_by_exprs(&mut self, exprs: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.order_by(exprs.inner.clone()));
        }
    }

    // -- LIMIT, OFFSET --

    /// Set the LIMIT clause.
    pub fn limit(&mut self, count: u32) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.limit(count as usize));
        }
    }

    /// Set the OFFSET clause.
    pub fn offset(&mut self, count: u32) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.offset(count as usize));
        }
    }

    // -- Modifiers --

    /// Enable DISTINCT.
    pub fn distinct(&mut self) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.distinct());
        }
    }

    /// Set the QUALIFY clause.
    pub fn qualify(&mut self, condition: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.qualify(condition.inner.clone()));
        }
    }

    /// Add a FOR UPDATE locking clause.
    pub fn for_update(&mut self) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.for_update());
        }
    }

    // -- SORT BY --

    /// Set the SORT BY clause (Hive/Spark).
    pub fn sort_by_exprs(&mut self, exprs: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.sort_by(exprs.inner.clone()));
        }
    }

    // -- WINDOW --

    /// Add a named WINDOW clause definition.
    pub fn window(&mut self, name: &str, def: &mut WasmWindowDefBuilder) -> Result<(), JsValue> {
        let window_def = take_owned(&mut def.inner, "WindowDefBuilder")?;
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.window(name, window_def));
        }
        Ok(())
    }

    // -- LATERAL VIEW --

    /// Add a LATERAL VIEW clause (Hive/Spark).
    pub fn lateral_view(
        &mut self,
        func_expr: &WasmExpr,
        table_alias: &str,
        col_aliases: Vec<String>,
    ) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.lateral_view(func_expr.inner.clone(), table_alias, col_aliases));
        }
    }

    // -- HINT --

    /// Add a query hint.
    pub fn hint(&mut self, hint_text: &str) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.hint(hint_text));
        }
    }

    // -- CTAS SQL --

    /// Convert to CREATE TABLE AS SELECT and return generated SQL.
    pub fn ctas_sql(&mut self, table_name: &str, dialect: &str) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "SelectBuilder")?;
        let expr = b.ctas(table_name);
        generate_sql(&expr, dialect)
    }

    // -- Set operations --

    /// Combine with UNION (duplicate elimination). Consumes both builders.
    pub fn union(&mut self, other: &mut WasmSelectBuilder) -> Result<WasmSetOpBuilder, JsValue> {
        let left = take_owned(&mut self.inner, "SelectBuilder")?;
        let right = take_owned(&mut other.inner, "SelectBuilder")?;
        Ok(WasmSetOpBuilder {
            inner: Some(left.union(right)),
        })
    }

    /// Combine with UNION ALL (keep duplicates). Consumes both builders.
    pub fn union_all(
        &mut self,
        other: &mut WasmSelectBuilder,
    ) -> Result<WasmSetOpBuilder, JsValue> {
        let left = take_owned(&mut self.inner, "SelectBuilder")?;
        let right = take_owned(&mut other.inner, "SelectBuilder")?;
        Ok(WasmSetOpBuilder {
            inner: Some(left.union_all(right)),
        })
    }

    /// Combine with INTERSECT. Consumes both builders.
    pub fn intersect(
        &mut self,
        other: &mut WasmSelectBuilder,
    ) -> Result<WasmSetOpBuilder, JsValue> {
        let left = take_owned(&mut self.inner, "SelectBuilder")?;
        let right = take_owned(&mut other.inner, "SelectBuilder")?;
        Ok(WasmSetOpBuilder {
            inner: Some(left.intersect(right)),
        })
    }

    /// Combine with EXCEPT. Consumes both builders.
    pub fn except_(&mut self, other: &mut WasmSelectBuilder) -> Result<WasmSetOpBuilder, JsValue> {
        let left = take_owned(&mut self.inner, "SelectBuilder")?;
        let right = take_owned(&mut other.inner, "SelectBuilder")?;
        Ok(WasmSetOpBuilder {
            inner: Some(left.except_(right)),
        })
    }

    // -- CTAS --

    /// Convert to CREATE TABLE AS SELECT. Returns the AST JSON.
    pub fn ctas(&mut self, table_name: &str) -> Result<JsValue, JsValue> {
        let b = take_owned(&mut self.inner, "SelectBuilder")?;
        let expr = b.ctas(table_name);
        build_json(&expr)
    }

    // -- Output --

    /// Generate SQL string for the given dialect.
    pub fn to_sql(&mut self, dialect: &str) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "SelectBuilder")?;
        let expr = b.build();
        generate_sql(&expr, dialect)
    }

    /// Return the Expression AST as a JSON value.
    pub fn build(&mut self) -> Result<JsValue, JsValue> {
        let b = take_owned(&mut self.inner, "SelectBuilder")?;
        let expr = b.build();
        build_json(&expr)
    }
}

// ---------------------------------------------------------------------------
// WasmInsertBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for INSERT INTO statements.
#[wasm_bindgen]
pub struct WasmInsertBuilder {
    inner: Option<CoreInsertBuilder>,
}

#[wasm_bindgen]
impl WasmInsertBuilder {
    /// Create a new INSERT INTO builder for the given table.
    #[wasm_bindgen(constructor)]
    pub fn new(table_name: &str) -> Self {
        WasmInsertBuilder {
            inner: Some(core_builder::insert_into(table_name)),
        }
    }

    /// Set the target column names.
    pub fn columns(&mut self, cols: Vec<String>) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.columns(cols));
        }
    }

    /// Append a row of values.
    pub fn values(&mut self, vals: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.values(vals.inner.clone()));
        }
    }

    /// Set the source query for INSERT ... SELECT.
    pub fn query(&mut self, query: &mut WasmSelectBuilder) -> Result<(), JsValue> {
        let q = take_owned(&mut query.inner, "SelectBuilder")?;
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.query(q));
        }
        Ok(())
    }

    /// Generate SQL string for the given dialect.
    pub fn to_sql(&mut self, dialect: &str) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "InsertBuilder")?;
        let expr = b.build();
        generate_sql(&expr, dialect)
    }

    /// Return the Expression AST as a JSON value.
    pub fn build(&mut self) -> Result<JsValue, JsValue> {
        let b = take_owned(&mut self.inner, "InsertBuilder")?;
        let expr = b.build();
        build_json(&expr)
    }
}

// ---------------------------------------------------------------------------
// WasmUpdateBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for UPDATE statements.
#[wasm_bindgen]
pub struct WasmUpdateBuilder {
    inner: Option<CoreUpdateBuilder>,
}

#[wasm_bindgen]
impl WasmUpdateBuilder {
    /// Create a new UPDATE builder for the given table.
    #[wasm_bindgen(constructor)]
    pub fn new(table_name: &str) -> Self {
        WasmUpdateBuilder {
            inner: Some(core_builder::update(table_name)),
        }
    }

    /// Add a SET column = value assignment.
    pub fn set(&mut self, column: &str, value: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.set(column, value.inner.clone()));
        }
    }

    /// Set the WHERE clause.
    pub fn where_expr(&mut self, condition: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.where_(condition.inner.clone()));
        }
    }

    /// Set the FROM clause (PostgreSQL/Snowflake UPDATE ... FROM).
    pub fn from(&mut self, table: &str) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.from(table));
        }
    }

    /// Generate SQL string for the given dialect.
    pub fn to_sql(&mut self, dialect: &str) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "UpdateBuilder")?;
        let expr = b.build();
        generate_sql(&expr, dialect)
    }

    /// Return the Expression AST as a JSON value.
    pub fn build(&mut self) -> Result<JsValue, JsValue> {
        let b = take_owned(&mut self.inner, "UpdateBuilder")?;
        let expr = b.build();
        build_json(&expr)
    }
}

// ---------------------------------------------------------------------------
// WasmDeleteBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for DELETE FROM statements.
#[wasm_bindgen]
pub struct WasmDeleteBuilder {
    inner: Option<CoreDeleteBuilder>,
}

#[wasm_bindgen]
impl WasmDeleteBuilder {
    /// Create a new DELETE FROM builder for the given table.
    #[wasm_bindgen(constructor)]
    pub fn new(table_name: &str) -> Self {
        WasmDeleteBuilder {
            inner: Some(core_builder::delete(table_name)),
        }
    }

    /// Set the WHERE clause.
    pub fn where_expr(&mut self, condition: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.where_(condition.inner.clone()));
        }
    }

    /// Generate SQL string for the given dialect.
    pub fn to_sql(&mut self, dialect: &str) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "DeleteBuilder")?;
        let expr = b.build();
        generate_sql(&expr, dialect)
    }

    /// Return the Expression AST as a JSON value.
    pub fn build(&mut self) -> Result<JsValue, JsValue> {
        let b = take_owned(&mut self.inner, "DeleteBuilder")?;
        let expr = b.build();
        build_json(&expr)
    }
}

// ---------------------------------------------------------------------------
// WasmMergeBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for MERGE INTO statements.
#[wasm_bindgen]
pub struct WasmMergeBuilder {
    inner: Option<CoreMergeBuilder>,
}

#[wasm_bindgen]
impl WasmMergeBuilder {
    /// Create a new MERGE INTO builder for the given target table.
    #[wasm_bindgen(constructor)]
    pub fn new(target: &str) -> Self {
        WasmMergeBuilder {
            inner: Some(core_builder::merge_into(target)),
        }
    }

    /// Set the source table and ON join condition.
    pub fn using(&mut self, source: &str, on: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.using(source, on.inner.clone()));
        }
    }

    /// Add a WHEN MATCHED THEN UPDATE SET clause.
    pub fn when_matched_update(&mut self, assignments: &WasmAssignmentArray) {
        if let Some(b) = self.inner.take() {
            let pairs: Vec<(&str, CoreExpr)> = assignments
                .inner
                .iter()
                .map(|(k, v)| (k.as_str(), v.clone()))
                .collect();
            self.inner = Some(b.when_matched_update(pairs));
        }
    }

    /// Add a WHEN MATCHED THEN DELETE clause.
    pub fn when_matched_delete(&mut self) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.when_matched_delete());
        }
    }

    /// Add a WHEN NOT MATCHED THEN INSERT clause.
    pub fn when_not_matched_insert(&mut self, columns: Vec<String>, values: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            let col_refs: Vec<&str> = columns.iter().map(|s| s.as_str()).collect();
            self.inner = Some(b.when_not_matched_insert(&col_refs, values.inner.clone()));
        }
    }

    /// Generate SQL string for the given dialect.
    pub fn to_sql(&mut self, dialect: &str) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "MergeBuilder")?;
        let expr = b.build();
        generate_sql(&expr, dialect)
    }

    /// Return the Expression AST as a JSON value.
    pub fn build(&mut self) -> Result<JsValue, JsValue> {
        let b = take_owned(&mut self.inner, "MergeBuilder")?;
        let expr = b.build();
        build_json(&expr)
    }
}

// ---------------------------------------------------------------------------
// WasmCaseBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for CASE expressions.
#[wasm_bindgen]
pub struct WasmCaseBuilder {
    inner: Option<CoreCaseBuilder>,
}

#[wasm_bindgen]
impl WasmCaseBuilder {
    /// Create a new searched CASE builder (CASE WHEN ... THEN ...).
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        WasmCaseBuilder {
            inner: Some(core_builder::case()),
        }
    }

    /// Add a WHEN condition THEN result branch.
    pub fn when(&mut self, condition: &WasmExpr, result: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.when(condition.inner.clone(), result.inner.clone()));
        }
    }

    /// Set the ELSE result.
    pub fn else_(&mut self, result: &WasmExpr) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.else_(result.inner.clone()));
        }
    }

    /// Build the CASE expression and return a WasmExpr.
    pub fn build_expr(&mut self) -> Result<WasmExpr, JsValue> {
        let b = take_owned(&mut self.inner, "CaseBuilder")?;
        Ok(WasmExpr::new(b.build()))
    }

    /// Generate SQL string (generic dialect).
    pub fn to_sql(&mut self) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "CaseBuilder")?;
        let expr = b.build();
        generate_sql(&expr.0, "generic")
    }
}

/// Create a simple CASE builder (CASE operand WHEN value THEN ...).
#[wasm_bindgen]
pub fn wasm_case_of(operand: &WasmExpr) -> WasmCaseBuilder {
    WasmCaseBuilder {
        inner: Some(core_builder::case_of(operand.inner.clone())),
    }
}

// ---------------------------------------------------------------------------
// WasmWindowDefBuilder
// ---------------------------------------------------------------------------

/// Builder for named WINDOW clause definitions.
#[wasm_bindgen]
pub struct WasmWindowDefBuilder {
    inner: Option<CoreWindowDefBuilder>,
}

#[wasm_bindgen]
impl WasmWindowDefBuilder {
    /// Create a new, empty window definition builder.
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        WasmWindowDefBuilder {
            inner: Some(CoreWindowDefBuilder::new()),
        }
    }

    /// Set the PARTITION BY expressions.
    pub fn partition_by(&mut self, exprs: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.partition_by(exprs.inner.clone()));
        }
    }

    /// Set the ORDER BY expressions.
    pub fn order_by(&mut self, exprs: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.order_by(exprs.inner.clone()));
        }
    }
}

// ---------------------------------------------------------------------------
// WasmSetOpBuilder
// ---------------------------------------------------------------------------

/// Fluent builder for set operations (UNION, INTERSECT, EXCEPT).
#[wasm_bindgen]
pub struct WasmSetOpBuilder {
    inner: Option<CoreSetOpBuilder>,
}

#[wasm_bindgen]
impl WasmSetOpBuilder {
    /// Set the ORDER BY clause.
    pub fn order_by_exprs(&mut self, exprs: &WasmExprArray) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.order_by(exprs.inner.clone()));
        }
    }

    /// Set the LIMIT clause.
    pub fn limit(&mut self, count: u32) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.limit(count as usize));
        }
    }

    /// Set the OFFSET clause.
    pub fn offset(&mut self, count: u32) {
        if let Some(b) = self.inner.take() {
            self.inner = Some(b.offset(count as usize));
        }
    }

    /// Generate SQL string for the given dialect.
    pub fn to_sql(&mut self, dialect: &str) -> Result<String, JsValue> {
        let b = take_owned(&mut self.inner, "SetOpBuilder")?;
        let expr = b.build();
        generate_sql(&expr, dialect)
    }

    /// Return the Expression AST as a JSON value.
    pub fn build(&mut self) -> Result<JsValue, JsValue> {
        let b = take_owned(&mut self.inner, "SetOpBuilder")?;
        let expr = b.build();
        build_json(&expr)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_col() {
        let c = wasm_col("users.id");
        assert_eq!(c.to_sql().unwrap(), "users.id");
    }

    // Note: wasm_lit() uses JsValue which only works on wasm32 targets.
    // Test literal creation via core builder directly.
    #[test]
    fn test_wasm_lit_string_via_core() {
        let l = WasmExpr::new(core_builder::lit("hello"));
        assert_eq!(l.to_sql().unwrap(), "'hello'");
    }

    #[test]
    fn test_wasm_lit_int_via_core() {
        let l = WasmExpr::new(core_builder::lit(42));
        assert_eq!(l.to_sql().unwrap(), "42");
    }

    #[test]
    fn test_wasm_lit_float_via_core() {
        let l = WasmExpr::new(core_builder::lit(3.14));
        assert_eq!(l.to_sql().unwrap(), "3.14");
    }

    #[test]
    fn test_wasm_lit_bool_via_core() {
        let l = WasmExpr::new(core_builder::boolean(true));
        assert_eq!(l.to_sql().unwrap(), "TRUE");
    }

    #[test]
    fn test_wasm_star() {
        let s = wasm_star();
        assert_eq!(s.to_sql().unwrap(), "*");
    }

    #[test]
    fn test_wasm_null() {
        let n = wasm_null();
        assert_eq!(n.to_sql().unwrap(), "NULL");
    }

    #[test]
    fn test_wasm_boolean() {
        let t = wasm_boolean(true);
        let f = wasm_boolean(false);
        assert_eq!(t.to_sql().unwrap(), "TRUE");
        assert_eq!(f.to_sql().unwrap(), "FALSE");
    }

    #[test]
    fn test_wasm_expr_comparison() {
        let left = wasm_col("age");
        let right = WasmExpr::new(core_builder::lit(18));
        let result = left.gte(&right);
        assert_eq!(result.to_sql().unwrap(), "age >= 18");
    }

    #[test]
    fn test_wasm_expr_logical() {
        let a = wasm_col("x");
        let b = WasmExpr::new(core_builder::lit(1));
        let c = wasm_col("y");
        let d = WasmExpr::new(core_builder::lit(2));
        let left = a.eq(&b);
        let right = c.eq(&d);
        let result = left.and_(&right);
        assert_eq!(result.to_sql().unwrap(), "x = 1 AND y = 2");
    }

    #[test]
    fn test_wasm_expr_arithmetic() {
        let a = wasm_col("price");
        let b = wasm_col("qty");
        let result = a.mul(&b);
        assert_eq!(result.to_sql().unwrap(), "price * qty");
    }

    #[test]
    fn test_wasm_expr_alias() {
        let e = wasm_col("name");
        let aliased = e.alias("n");
        assert_eq!(aliased.to_sql().unwrap(), "name AS n");
    }

    #[test]
    fn test_wasm_expr_cast() {
        let e = wasm_col("id");
        let casted = e.cast("VARCHAR");
        assert_eq!(casted.to_sql().unwrap(), "CAST(id AS VARCHAR)");
    }

    #[test]
    fn test_wasm_expr_asc_desc() {
        let a = wasm_col("name").asc();
        let d = wasm_col("age").desc();
        assert_eq!(a.to_sql().unwrap(), "name ASC");
        assert_eq!(d.to_sql().unwrap(), "age DESC");
    }

    #[test]
    fn test_wasm_expr_like() {
        let e = wasm_col("name");
        let p = WasmExpr::new(core_builder::lit("%test%"));
        let result = e.like(&p);
        assert_eq!(result.to_sql().unwrap(), "name LIKE '%test%'");
    }

    #[test]
    fn test_wasm_expr_is_null() {
        let e = wasm_col("x");
        assert_eq!(e.is_null().to_sql().unwrap(), "x IS NULL");
    }

    #[test]
    fn test_wasm_expr_is_not_null() {
        let e = wasm_col("x");
        assert_eq!(e.is_not_null().to_sql().unwrap(), "NOT x IS NULL");
    }

    #[test]
    fn test_wasm_expr_between() {
        let e = wasm_col("age");
        let low = WasmExpr::new(core_builder::lit(18));
        let high = WasmExpr::new(core_builder::lit(65));
        let result = e.between(&low, &high);
        assert_eq!(result.to_sql().unwrap(), "age BETWEEN 18 AND 65");
    }

    #[test]
    fn test_wasm_expr_in_list() {
        let e = wasm_col("status");
        let mut arr = WasmExprArray::new();
        arr.push_str("active");
        arr.push_str("pending");
        let result = e.in_list(&arr);
        assert_eq!(result.to_sql().unwrap(), "status IN ('active', 'pending')");
    }

    #[test]
    fn test_wasm_func() {
        let mut args = WasmExprArray::new();
        args.push(&wasm_col("name"));
        let result = wasm_func("UPPER", &args);
        assert_eq!(result.to_sql().unwrap(), "UPPER(name)");
    }

    #[test]
    fn test_wasm_select_builder_basic() {
        let mut b = WasmSelectBuilder::new();
        b.select_col("id");
        b.select_col("name");
        b.from("users");
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(sql, "SELECT id, name FROM users");
    }

    #[test]
    fn test_wasm_select_builder_where() {
        let mut b = WasmSelectBuilder::new();
        b.select_star();
        b.from("users");
        let cond = wasm_col("id").eq(&WasmExpr::new(core_builder::lit(1)));
        b.where_expr(&cond);
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(sql, "SELECT * FROM users WHERE id = 1");
    }

    #[test]
    fn test_wasm_select_builder_not_in_is_canonical_in_generic() {
        let mut b = WasmSelectBuilder::new();
        b.select_col("id");
        b.from("users");
        let mut arr = WasmExprArray::new();
        arr.push_str("deleted");
        arr.push_str("banned");
        let cond = wasm_col("status").not_in(&arr);
        b.where_expr(&cond);
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(
            sql,
            "SELECT id FROM users WHERE status NOT IN ('deleted', 'banned')"
        );
    }

    #[test]
    fn test_wasm_select_builder_to_sql_returns_error_when_consumed() {
        let mut b = WasmSelectBuilder::new();
        b.select_star();
        b.from("users");
        let _ = b.to_sql("generic").unwrap();
        assert!(b.to_sql("generic").is_err());
    }

    #[test]
    fn test_wasm_select_builder_order_limit() {
        let mut b = WasmSelectBuilder::new();
        b.select_col("name");
        b.from("users");
        let mut order = WasmExprArray::new();
        order.push(&wasm_col("name").asc());
        b.order_by_exprs(&order);
        b.limit(10);
        b.offset(5);
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(
            sql,
            "SELECT name FROM users ORDER BY name ASC LIMIT 10 OFFSET 5"
        );
    }

    #[test]
    fn test_wasm_select_builder_join() {
        let mut b = WasmSelectBuilder::new();
        b.select_col("u.id");
        b.from("users");
        let on = wasm_col("u.id").eq(&wasm_col("o.user_id"));
        b.left_join("orders", &on);
        let sql = b.to_sql("generic").unwrap();
        assert!(sql.contains("LEFT JOIN orders ON u.id = o.user_id"));
    }

    #[test]
    fn test_wasm_select_builder_group_by_having() {
        let mut b = WasmSelectBuilder::new();
        b.select_col("dept");
        let mut count_args = WasmExprArray::new();
        count_args.push_star();
        let count = wasm_func("COUNT", &count_args).alias("cnt");
        b.select_expr(&count);
        b.from("employees");
        let mut group = WasmExprArray::new();
        group.push_col("dept");
        b.group_by_cols(&group);
        let mut having_args = WasmExprArray::new();
        having_args.push_star();
        let having_cond = wasm_func("COUNT", &having_args).gt(&WasmExpr::new(core_builder::lit(5)));
        b.having(&having_cond);
        let sql = b.to_sql("generic").unwrap();
        assert!(sql.contains("GROUP BY dept"));
        assert!(sql.contains("HAVING COUNT(*) > 5"));
    }

    #[test]
    fn test_wasm_select_builder_distinct() {
        let mut b = WasmSelectBuilder::new();
        b.select_col("name");
        b.from("users");
        b.distinct();
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(sql, "SELECT DISTINCT name FROM users");
    }

    #[test]
    #[cfg(feature = "dialect-tsql")]
    fn test_wasm_select_builder_dialect() {
        let mut b = WasmSelectBuilder::new();
        b.select_star();
        b.from("users");
        b.limit(10);
        let sql = b.to_sql("tsql").unwrap();
        // TSQL uses TOP instead of LIMIT
        assert!(sql.contains("TOP 10") || sql.contains("FETCH"));
    }

    #[test]
    fn test_wasm_insert_builder() {
        let mut b = WasmInsertBuilder::new("users");
        b.columns(vec!["id".to_string(), "name".to_string()]);
        let mut vals = WasmExprArray::new();
        vals.push_int(1);
        vals.push_str("Alice");
        b.values(&vals);
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(sql, "INSERT INTO users (id, name) VALUES (1, 'Alice')");
    }

    #[test]
    fn test_wasm_insert_select() {
        let mut b = WasmInsertBuilder::new("archive");
        b.columns(vec!["id".to_string(), "name".to_string()]);
        let mut q = WasmSelectBuilder::new();
        q.select_col("id");
        q.select_col("name");
        q.from("users");
        b.query(&mut q).unwrap();
        let sql = b.to_sql("generic").unwrap();
        assert!(sql.contains("INSERT INTO archive"));
        assert!(sql.contains("SELECT id, name FROM users"));
    }

    #[test]
    fn test_wasm_update_builder() {
        let mut b = WasmUpdateBuilder::new("users");
        b.set("name", &WasmExpr::new(core_builder::lit("Bob")));
        let cond = wasm_col("id").eq(&WasmExpr::new(core_builder::lit(1)));
        b.where_expr(&cond);
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(sql, "UPDATE users SET name = 'Bob' WHERE id = 1");
    }

    #[test]
    fn test_wasm_delete_builder() {
        let mut b = WasmDeleteBuilder::new("users");
        let cond = wasm_col("id").eq(&WasmExpr::new(core_builder::lit(1)));
        b.where_expr(&cond);
        let sql = b.to_sql("generic").unwrap();
        assert_eq!(sql, "DELETE FROM users WHERE id = 1");
    }

    #[test]
    fn test_wasm_case_builder() {
        let mut b = WasmCaseBuilder::new();
        let cond = wasm_col("x").gt(&WasmExpr::new(core_builder::lit(0)));
        let result = WasmExpr::new(core_builder::lit("positive"));
        b.when(&cond, &result);
        let else_result = WasmExpr::new(core_builder::lit("non-positive"));
        b.else_(&else_result);
        let sql = b.to_sql().unwrap();
        assert_eq!(
            sql,
            "CASE WHEN x > 0 THEN 'positive' ELSE 'non-positive' END"
        );
    }

    #[test]
    fn test_wasm_case_of() {
        let operand = wasm_col("status");
        let mut b = wasm_case_of(&operand);
        b.when(
            &WasmExpr::new(core_builder::lit(1)),
            &WasmExpr::new(core_builder::lit("active")),
        );
        b.when(
            &WasmExpr::new(core_builder::lit(0)),
            &WasmExpr::new(core_builder::lit("inactive")),
        );
        let sql = b.to_sql().unwrap();
        assert_eq!(
            sql,
            "CASE status WHEN 1 THEN 'active' WHEN 0 THEN 'inactive' END"
        );
    }

    #[test]
    fn test_wasm_set_op_union() {
        let mut a = WasmSelectBuilder::new();
        a.select_col("id");
        a.from("a");
        let mut b = WasmSelectBuilder::new();
        b.select_col("id");
        b.from("b");
        let mut set_op = a.union_all(&mut b).unwrap();
        let mut order = WasmExprArray::new();
        order.push_col("id");
        set_op.order_by_exprs(&order);
        set_op.limit(10);
        let sql = set_op.to_sql("generic").unwrap();
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("LIMIT 10"));
    }

    #[test]
    fn test_wasm_merge_builder() {
        let mut b = WasmMergeBuilder::new("target");
        let on_cond = wasm_col("target.id").eq(&wasm_col("source.id"));
        b.using("source", &on_cond);

        let mut assignments = WasmAssignmentArray::new();
        assignments.push("name", &wasm_col("source.name"));
        b.when_matched_update(&assignments);

        b.when_matched_delete();

        let mut vals = WasmExprArray::new();
        vals.push(&wasm_col("source.id"));
        vals.push(&wasm_col("source.name"));
        b.when_not_matched_insert(vec!["id".to_string(), "name".to_string()], &vals);

        let sql = b.to_sql("generic").unwrap();
        assert!(sql.contains("MERGE INTO"));
        assert!(sql.contains("USING source ON"));
        assert!(sql.contains("WHEN MATCHED"));
    }

    #[test]
    fn test_wasm_expr_not() {
        let e = wasm_col("active");
        let result = e.not();
        assert_eq!(result.to_sql().unwrap(), "NOT active");
    }

    #[test]
    fn test_wasm_expr_xor() {
        let a = wasm_col("a");
        let b = wasm_col("b");
        let result = a.xor(&b);
        assert_eq!(result.to_sql().unwrap(), "a XOR b");
    }

    #[test]
    fn test_wasm_expr_not_in() {
        let e = wasm_col("x");
        let mut arr = WasmExprArray::new();
        arr.push_int(1);
        arr.push_int(2);
        arr.push_int(3);
        let result = e.not_in(&arr);
        assert_eq!(result.to_sql().unwrap(), "x NOT IN (1, 2, 3)");
    }

    #[test]
    fn test_wasm_sql_expr() {
        let e = wasm_sql_expr("COALESCE(a, b, 0)");
        assert_eq!(e.to_sql().unwrap(), "COALESCE(a, b, 0)");
    }

    #[test]
    fn test_wasm_count_distinct() {
        let e = wasm_count_distinct(&wasm_col("x"));
        assert_eq!(e.to_sql().unwrap(), "COUNT(DISTINCT x)");
    }

    #[test]
    fn test_wasm_extract() {
        let e = wasm_extract("YEAR", &wasm_col("created_at"));
        assert_eq!(e.to_sql().unwrap(), "EXTRACT(YEAR FROM created_at)");
    }

    #[test]
    fn test_wasm_select_where_sql() {
        let mut b = WasmSelectBuilder::new();
        b.select_star();
        b.from("users");
        b.where_sql("age > 18 AND status = 'active'");
        let sql = b.to_sql("generic").unwrap();
        assert!(sql.contains("WHERE age > 18 AND status = 'active'"));
    }

    #[test]
    fn test_wasm_expr_array() {
        let mut arr = WasmExprArray::new();
        assert_eq!(arr.len(), 0);
        arr.push_col("a");
        arr.push_str("hello");
        arr.push_int(42);
        arr.push_float(3.14);
        arr.push_star();
        arr.push(&wasm_col("b"));
        assert_eq!(arr.len(), 6);
    }

    #[test]
    fn test_wasm_free_functions() {
        let and_result = wasm_and(&wasm_col("a"), &wasm_col("b"));
        assert_eq!(and_result.to_sql().unwrap(), "a AND b");

        let or_result = wasm_or(&wasm_col("a"), &wasm_col("b"));
        assert_eq!(or_result.to_sql().unwrap(), "a OR b");

        let not_result = wasm_not(&wasm_col("a"));
        assert_eq!(not_result.to_sql().unwrap(), "NOT a");

        let cast_result = wasm_cast(&wasm_col("x"), "INT");
        assert_eq!(cast_result.to_sql().unwrap(), "CAST(x AS INT)");

        let alias_result = wasm_alias(&wasm_col("x"), "y");
        assert_eq!(alias_result.to_sql().unwrap(), "x AS y");
    }
}
