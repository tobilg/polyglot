use crate::errors::{map_generate_error, GenerateError};
use crate::expr_types::{wrap_expression, wrap_expression_with_parent};
use crate::helpers::{resolve_dialect, to_python_object};
use polyglot_sql::traversal::ExpressionWalk;
use polyglot_sql::Expression;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyString, PyTuple};
use serde_json::Value;

#[pyclass(subclass, from_py_object, module = "polyglot_sql", name = "Expression")]
#[derive(Debug)]
pub struct PyExpression {
    pub(crate) inner: Expression,
    pub(crate) parent: Option<Py<PyAny>>,
    pub(crate) arg_key: Option<String>,
}

impl Clone for PyExpression {
    fn clone(&self) -> Self {
        let parent = self
            .parent
            .as_ref()
            .and_then(|p| Python::try_attach(|py| p.clone_ref(py)));
        Self {
            inner: self.inner.clone(),
            parent,
            arg_key: self.arg_key.clone(),
        }
    }
}

impl PyExpression {
    pub fn from_inner(inner: Expression) -> Self {
        Self {
            inner,
            parent: None,
            arg_key: None,
        }
    }

    pub fn from_inner_with_parent(inner: Expression, parent: Py<PyAny>, arg_key: &str) -> Self {
        Self {
            inner,
            parent: Some(parent),
            arg_key: Some(arg_key.to_string()),
        }
    }
}

fn expression_value(expr: &Expression) -> PyResult<Value> {
    serde_json::to_value(expr)
        .map_err(|err| GenerateError::new_err(format!("Failed to serialize expression: {err}")))
}

fn expression_payload(expr: &Expression) -> PyResult<Option<serde_json::Map<String, Value>>> {
    match expression_value(expr)? {
        Value::Object(map) => Ok(map.into_values().next().and_then(|value| match value {
            Value::Object(payload) => Some(payload),
            _ => None,
        })),
        _ => Ok(None),
    }
}

fn normalize_kind(kind: &str) -> String {
    kind.trim().to_ascii_lowercase()
}

/// Convert a PascalCase class name to snake_case for matching against variant_name().
fn pascal_to_snake(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if i > 0 && ch.is_uppercase() {
            result.push('_');
        }
        result.push(ch.to_ascii_lowercase());
    }
    result
}

/// Extract snake_case kind strings from a PyTuple of type objects or strings.
fn extract_kinds(args: &Bound<'_, PyTuple>) -> PyResult<Vec<String>> {
    let mut kinds = Vec::new();
    for arg in args.iter() {
        if arg.is_instance_of::<PyString>() {
            kinds.push(normalize_kind(&arg.extract::<String>()?));
        } else if arg.is_callable() {
            // Type object — get __name__ and convert PascalCase to snake_case
            let name: String = arg.getattr("__name__")?.extract()?;
            kinds.push(pascal_to_snake(&name));
        } else {
            return Err(PyValueError::new_err(
                "find()/find_all() arguments must be type objects or strings",
            ));
        }
    }
    Ok(kinds)
}

/// Convert a snake_case name to PascalCase for display (e.g. "select" -> "Select",
/// "data_type" -> "DataType").
fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(c) => {
                    let mut s = c.to_uppercase().to_string();
                    s.extend(chars);
                    s
                }
                None => String::new(),
            }
        })
        .collect()
}

/// Check if a JSON value looks like a tagged Expression variant (single-key object
/// whose key wraps an object or null — the serde representation of a Rust enum variant).
fn is_expression_variant(value: &Value) -> bool {
    if let Value::Object(map) = value {
        if map.len() == 1 {
            let inner = map.values().next().unwrap();
            return matches!(inner, Value::Object(_) | Value::Null);
        }
    }
    false
}

/// Format a serde_json::Value as a sqlglot-style repr tree.
fn format_repr(value: &Value, indent: usize) -> String {
    match value {
        // Tagged enum variant: {"select": {...}} -> Select(...)
        Value::Object(map) if is_expression_variant(value) => {
            let (key, inner) = map.iter().next().unwrap();
            let type_name = to_pascal_case(key);
            match inner {
                Value::Object(fields) => format_typed(Some(&type_name), fields, indent),
                Value::Null => type_name,
                _ => format!("{type_name}({})", format_scalar(inner)),
            }
        }
        // Plain struct object — render without type name
        Value::Object(map) => format_typed(None, map, indent),
        Value::Array(arr) => format_array(arr, indent),
        _ => format_scalar(value),
    }
}

/// Format struct fields, optionally prefixed with a type name.
fn format_typed(
    type_name: Option<&str>,
    fields: &serde_json::Map<String, Value>,
    indent: usize,
) -> String {
    let parts = collect_fields(fields, indent);

    // No visible fields
    if parts.is_empty() {
        return type_name.unwrap_or("").to_string();
    }

    let all_inline = parts.iter().all(|(_, v)| !v.contains('\n'));
    let inline_str = || {
        parts
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let block_str = || {
        let pad = "  ".repeat(indent + 1);
        parts
            .iter()
            .map(|(k, v)| format!("{pad}{k}={v}"))
            .collect::<Vec<_>>()
            .join(",\n")
    };

    match type_name {
        Some(name) => {
            if all_inline {
                format!("{name}({inline})", inline = inline_str())
            } else {
                format!("{name}(\n{body})", body = block_str())
            }
        }
        None => {
            // Anonymous struct: wrap in parens to avoid confusion with parent fields
            if all_inline {
                format!("({})", inline_str())
            } else {
                format!("(\n{})", block_str())
            }
        }
    }
}

/// Collect non-empty fields as (key, formatted_value) pairs.
fn collect_fields(fields: &serde_json::Map<String, Value>, indent: usize) -> Vec<(String, String)> {
    fields
        .iter()
        .filter(|(_, v)| !should_skip(v))
        .map(|(k, v)| (k.clone(), format_repr(v, indent + 1)))
        .collect()
}

/// Format an array value.
fn format_array(arr: &[Value], indent: usize) -> String {
    if arr.is_empty() {
        return "[]".to_string();
    }

    let pad = "  ".repeat(indent + 1);
    let items: Vec<String> = arr
        .iter()
        .map(|item| format!("{pad}{}", format_repr(item, indent + 1)))
        .collect();

    format!("[\n{}]", items.join(",\n"))
}

/// Format a scalar value inline.
fn format_scalar(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        Value::Null => "None".to_string(),
        _ => format!("{value}"),
    }
}

/// Whether a field should be skipped in repr output.
fn should_skip(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Bool(false) => true,
        Value::Array(arr) => arr.is_empty(),
        Value::String(s) => s.is_empty(),
        _ => false,
    }
}

fn value_to_python_object(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    if let Ok(expr) = serde_json::from_value::<Expression>(value.clone()) {
        return wrap_expression(py, expr);
    }

    if let Ok(expressions) = serde_json::from_value::<Vec<Expression>>(value.clone()) {
        let objects = expressions
            .into_iter()
            .map(|expr| wrap_expression(py, expr))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(pyo3::types::PyList::new(py, &objects)?.unbind().into_any());
    }

    to_python_object(py, &value)
}

#[pymethods]
impl PyExpression {
    // === Core accessors ===

    #[getter]
    fn kind(&self) -> String {
        self.inner.variant_name().to_string()
    }

    #[getter]
    fn key(&self) -> String {
        self.inner.variant_name().to_string()
    }

    #[getter]
    fn tree_depth(&self) -> usize {
        self.inner.tree_depth()
    }

    #[pyo3(signature = (dialect = None, *, pretty = false))]
    fn sql(&self, dialect: Option<&str>, pretty: bool) -> PyResult<String> {
        let dialect = resolve_dialect(dialect.unwrap_or("generic"))?;

        if pretty {
            dialect.generate_pretty(&self.inner)
        } else {
            dialect.generate(&self.inner)
        }
        .map_err(map_generate_error)
    }

    fn to_dict(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        to_python_object(py, &self.inner)
    }

    fn arg(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyAny>> {
        if let Some(payload) = expression_payload(&self.inner)? {
            if let Some(value) = payload.get(name) {
                return value_to_python_object(py, value.clone());
            }
        }

        Ok(py.None())
    }

    // === Property accessors (no serde) ===

    /// Returns the primary child expression (".this" in sqlglot).
    #[getter]
    fn this(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let inner = &slf.borrow().inner;
        match inner.get_this() {
            Some(child) => {
                let parent_ref = slf.unbind().into_any();
                wrap_expression_with_parent(py, child.clone(), parent_ref, "this").map(Some)
            }
            None => Ok(None),
        }
    }

    /// Returns the secondary child expression (".expression" in sqlglot).
    #[getter]
    fn expression(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let inner = &slf.borrow().inner;
        match inner.get_expression() {
            Some(child) => {
                let parent_ref = slf.unbind().into_any();
                wrap_expression_with_parent(py, child.clone(), parent_ref, "expression").map(Some)
            }
            None => Ok(None),
        }
    }

    /// Returns the list children (".expressions" in sqlglot).
    #[getter]
    fn expressions(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let inner = &slf.borrow().inner;
        let exprs = inner.get_expressions();
        if exprs.is_empty() {
            return Ok(Vec::new());
        }
        let parent_ref = slf.unbind().into_any();
        exprs
            .iter()
            .map(|child| {
                wrap_expression_with_parent(
                    py,
                    child.clone(),
                    parent_ref.clone_ref(py),
                    "expressions",
                )
            })
            .collect()
    }

    /// Returns the name of this expression.
    #[getter]
    fn name(&self) -> String {
        self.inner.get_name().to_string()
    }

    /// Returns the alias of this expression.
    #[getter]
    fn alias(&self) -> String {
        self.inner.get_alias().to_string()
    }

    /// Returns alias if non-empty, otherwise name.
    #[getter]
    fn alias_or_name(&self) -> String {
        let a = self.inner.get_alias();
        if !a.is_empty() {
            a.to_string()
        } else {
            self.inner.get_name().to_string()
        }
    }

    /// Returns the output name (what a column shows up as in SELECT).
    #[getter]
    fn output_name(&self) -> String {
        self.inner.get_output_name().to_string()
    }

    /// Returns comments attached to this expression.
    #[getter]
    fn comments(&self) -> Vec<String> {
        self.inner
            .get_comments()
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    /// Returns the args dict (serde path, not hot).
    #[getter]
    fn args(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(payload) = expression_payload(&self.inner)? {
            let dict = pyo3::types::PyDict::new(py);
            for (key, value) in payload {
                let py_val = value_to_python_object(py, value)?;
                dict.set_item(key, py_val)?;
            }
            Ok(dict.unbind().into_any())
        } else {
            Ok(pyo3::types::PyDict::new(py).unbind().into_any())
        }
    }

    /// Returns the inferred type as a DataType Expression, if present.
    #[getter]
    fn type_(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.inner.inferred_type() {
            Some(dt) => {
                let expr = Expression::DataType(dt.clone());
                wrap_expression(py, expr).map(Some)
            }
            None => Ok(None),
        }
    }

    // === Type predicates ===

    #[getter]
    fn is_string(&self) -> bool {
        matches!(&self.inner, Expression::Literal(lit) if lit.is_string())
    }

    #[getter]
    fn is_number(&self) -> bool {
        match &self.inner {
            Expression::Literal(lit) => lit.is_number(),
            Expression::Neg(u) => matches!(&u.this, Expression::Literal(lit) if lit.is_number()),
            _ => false,
        }
    }

    #[getter]
    fn is_int(&self) -> bool {
        let check_int = |lit: &polyglot_sql::expressions::Literal| {
            if let polyglot_sql::expressions::Literal::Number(s) = lit {
                s.parse::<i64>().is_ok()
            } else {
                false
            }
        };
        match &self.inner {
            Expression::Literal(lit) => check_int(lit),
            Expression::Neg(u) => {
                matches!(&u.this, Expression::Literal(lit) if check_int(lit))
            }
            _ => false,
        }
    }

    #[getter]
    fn is_star(&self) -> bool {
        match &self.inner {
            Expression::Star(_) => true,
            Expression::Column(c) => {
                matches!(&Expression::Identifier(c.name.clone()), Expression::Identifier(id) if id.name == "*")
            }
            _ => false,
        }
    }

    fn is_leaf(&self) -> bool {
        self.inner.children().is_empty()
    }

    // === Parent tracking ===

    #[getter]
    fn parent(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.parent.as_ref().map(|p| p.clone_ref(py))
    }

    #[getter]
    fn depth(&self, py: Python<'_>) -> PyResult<usize> {
        let mut depth = 0;
        let mut current_parent = self.parent.as_ref().map(|p| p.clone_ref(py));
        while let Some(p) = current_parent {
            depth += 1;
            let bound = p.bind(py);
            let py_expr: PyRef<'_, PyExpression> = bound.extract()?;
            current_parent = py_expr.parent.as_ref().map(|pp| pp.clone_ref(py));
        }
        Ok(depth)
    }

    fn root(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut current: Py<PyAny> = slf.unbind().into_any();
        loop {
            let bound = current.bind(py);
            let py_expr: PyRef<'_, PyExpression> = bound.extract()?;
            match &py_expr.parent {
                Some(p) => {
                    let next = p.clone_ref(py);
                    drop(py_expr);
                    current = next;
                }
                None => {
                    drop(py_expr);
                    return Ok(current);
                }
            }
        }
    }

    #[pyo3(signature = (*types))]
    fn find_ancestor(
        &self,
        py: Python<'_>,
        types: &Bound<'_, PyTuple>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let kinds = extract_kinds(types)?;
        let mut current_parent = self.parent.as_ref().map(|p| p.clone_ref(py));
        while let Some(p) = current_parent {
            let bound = p.bind(py);
            let py_expr: PyRef<'_, PyExpression> = bound.extract()?;
            let vname = py_expr.inner.variant_name();
            if kinds.iter().any(|k| k == vname) {
                drop(py_expr);
                return Ok(Some(p));
            }
            let next = py_expr.parent.as_ref().map(|pp| pp.clone_ref(py));
            drop(py_expr);
            current_parent = next;
        }
        Ok(None)
    }

    #[getter]
    fn parent_select(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let mut current_parent = self.parent.as_ref().map(|p| p.clone_ref(py));
        while let Some(p) = current_parent {
            let bound = p.bind(py);
            let py_expr: PyRef<'_, PyExpression> = bound.extract()?;
            if py_expr.inner.variant_name() == "select" {
                drop(py_expr);
                return Ok(Some(p));
            }
            let next = py_expr.parent.as_ref().map(|pp| pp.clone_ref(py));
            drop(py_expr);
            current_parent = next;
        }
        Ok(None)
    }

    // === Traversal ===

    fn children(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let inner = &slf.borrow().inner;
        let rust_children: Vec<Expression> = inner.children().into_iter().cloned().collect();
        let parent_ref = slf.unbind().into_any();
        rust_children
            .into_iter()
            .map(|expr| wrap_expression_with_parent(py, expr, parent_ref.clone_ref(py), "child"))
            .collect()
    }

    #[pyo3(signature = (order = "dfs"))]
    fn walk(slf: Bound<'_, Self>, py: Python<'_>, order: &str) -> PyResult<Vec<Py<PyAny>>> {
        let order_lower = order.trim().to_ascii_lowercase();
        let inner = &slf.borrow().inner;
        let nodes: Vec<Expression> = match order_lower.as_str() {
            "dfs" => inner.dfs().cloned().collect(),
            "bfs" => inner.bfs().cloned().collect(),
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported walk order: {order}"
                )))
            }
        };
        // The root node gets no parent from walk; children get parent set during
        // subsequent property access. We wrap without parent for walk results
        // to avoid O(n^2) parent-chain computation.
        nodes
            .into_iter()
            .map(|expr| wrap_expression(py, expr))
            .collect()
    }

    #[pyo3(signature = (*types))]
    fn find(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        types: &Bound<'_, PyTuple>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let kinds = extract_kinds(types)?;
        let inner = &slf.borrow().inner;
        // Skip self (DFS starts with self) — sqlglot find skips root
        let found = inner
            .dfs()
            .skip(1)
            .find(|expr| {
                let vname = expr.variant_name();
                kinds.iter().any(|k| k == vname)
            })
            .cloned();
        match found {
            Some(expr) => {
                // We don't set parent here for perf; parent is set lazily via property access
                wrap_expression(py, expr).map(Some)
            }
            None => Ok(None),
        }
    }

    #[pyo3(signature = (*types))]
    fn find_all(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        types: &Bound<'_, PyTuple>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let kinds = extract_kinds(types)?;
        let inner = &slf.borrow().inner;
        let matches: Vec<Expression> = inner
            .dfs()
            .skip(1)
            .filter(|expr| {
                let vname = expr.variant_name();
                kinds.iter().any(|k| k == vname)
            })
            .cloned()
            .collect();
        matches
            .into_iter()
            .map(|expr| wrap_expression(py, expr))
            .collect()
    }

    /// Unwrap nested Paren expressions.
    fn unnest(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = &slf.borrow().inner;
        let mut current = inner;
        while let Expression::Paren(p) = current {
            current = &p.this;
        }
        if std::ptr::eq(current, inner) {
            Ok(slf.unbind().into_any())
        } else {
            wrap_expression(py, current.clone())
        }
    }

    /// Unwrap Alias expression.
    fn unalias(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let inner = &slf.borrow().inner;
        if let Expression::Alias(a) = inner {
            wrap_expression(py, a.this.clone())
        } else {
            Ok(slf.unbind().into_any())
        }
    }

    /// Flatten same-variant chains (e.g., And(And(a,b),c) → [a,b,c]).
    fn flatten(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let target_kind = self.inner.variant_name();
        let mut result = Vec::new();
        let mut stack = vec![&self.inner];
        while let Some(expr) = stack.pop() {
            if expr.variant_name() == target_kind {
                // Push children in reverse so left comes out first
                if let Some(right) = expr.get_expression() {
                    stack.push(right);
                }
                if let Some(left) = expr.get_this() {
                    stack.push(left);
                }
            } else {
                result.push(wrap_expression(py, expr.clone())?);
            }
        }
        Ok(result)
    }

    /// Alias for children() — sqlglot compat.
    fn iter_expressions(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        PyExpression::children(slf, py)
    }

    /// Get text for a given arg key.
    fn text(&self, py: Python<'_>, key: &str) -> PyResult<String> {
        if let Some(payload) = expression_payload(&self.inner)? {
            if let Some(value) = payload.get(key) {
                return match value {
                    Value::String(s) => Ok(s.clone()),
                    Value::Number(n) => Ok(n.to_string()),
                    Value::Bool(b) => Ok(b.to_string()),
                    Value::Null => Ok(String::new()),
                    Value::Object(map) => {
                        // Could be a tagged Expression variant or an inline struct (like Identifier)
                        if let Ok(expr) = serde_json::from_value::<Expression>(value.clone()) {
                            return Ok(expr.get_name().to_string());
                        }
                        // Inline struct — look for a "name" key
                        if let Some(Value::String(s)) = map.get("name") {
                            return Ok(s.clone());
                        }
                        Ok(String::new())
                    }
                    _ => Ok(String::new()),
                };
            }
        }
        let _ = py;
        Ok(String::new())
    }

    // === Standard Python methods ===

    fn __eq__(&self, other: &Self) -> bool {
        self.inner == other.inner
    }

    fn __str__(&self) -> PyResult<String> {
        let dialect = resolve_dialect("generic")?;
        dialect.generate(&self.inner).map_err(map_generate_error)
    }

    fn __repr__(&self) -> PyResult<String> {
        let value = expression_value(&self.inner)?;
        Ok(format_repr(&value, 0))
    }
}
