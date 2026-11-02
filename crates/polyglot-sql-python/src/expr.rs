use crate::builders::{
    apply_and_wrap, binary_operator, join_type as parse_join_type, node_from_any, nodes_from_tuple,
    unary_operator,
};
use crate::errors::{map_generate_error, GenerateError};
use crate::expr_types::{wrap_expression, wrap_shared_expression};
use crate::helpers::{resolve_dialect, to_python_object};
use crate::shared_expression::SharedExpression;
use polyglot_sql::ast_json;
use polyglot_sql::builder::plan::{BuildNode, BuildOperation, BuilderAssignment, BuilderPlan};
use polyglot_sql::traversal::ExpressionWalk;
use polyglot_sql::Expression;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyString, PyTuple};
use serde_json::Value;

#[pyclass(subclass, from_py_object, module = "polyglot_sql", name = "Expression")]
#[derive(Debug)]
pub struct PyExpression {
    pub(crate) inner: SharedExpression,
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
            inner: self.inner.share(),
            parent,
            arg_key: self.arg_key.clone(),
        }
    }
}

impl PyExpression {
    pub fn from_inner(inner: Expression) -> Self {
        Self {
            inner: SharedExpression::new(inner),
            parent: None,
            arg_key: None,
        }
    }

    fn builder_binary(
        &self,
        py: Python<'_>,
        op: &str,
        other: &Bound<'_, PyAny>,
        parse_string: bool,
    ) -> PyResult<Py<PyAny>> {
        let node = BuildNode::Binary {
            op: binary_operator(op)?,
            left: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
            right: Box::new(node_from_any(other, parse_string)?),
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    fn builder_unary(&self, py: Python<'_>, op: &str) -> PyResult<Py<PyAny>> {
        let node = BuildNode::Unary {
            op: unary_operator(op)?,
            expression: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    fn builder_ordered(&self, py: Python<'_>, desc: bool) -> PyResult<Py<PyAny>> {
        let node = BuildNode::Ordered {
            expression: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
            desc,
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    fn builder_list_operation(
        &self,
        py: Python<'_>,
        kind: &str,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        let expressions = nodes_from_tuple(expressions, true)?;
        let operation = match kind {
            "where" => BuildOperation::Where {
                expressions,
                append,
            },
            "group_by" => BuildOperation::GroupBy {
                expressions,
                append,
            },
            "having" => BuildOperation::Having {
                expressions,
                append,
            },
            "order_by" => BuildOperation::OrderBy {
                expressions,
                append,
            },
            "sort_by" => BuildOperation::SortBy {
                expressions,
                append,
            },
            "qualify" => BuildOperation::Qualify {
                expressions,
                append,
            },
            _ => unreachable!("known builder list operation"),
        };
        apply_and_wrap(py, self, operation, dialect, copy)
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
    if let Ok(expr) = ast_json::expression_from_value(value.clone()) {
        return wrap_expression(py, expr);
    }

    if let Ok(expressions) = ast_json::expressions_from_value(value.clone()) {
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
        let child = slf.borrow().inner.select(Expression::get_this);
        child
            .map(|child| {
                wrap_shared_expression(py, child, Some(slf.unbind().into_any()), Some("this"))
            })
            .transpose()
    }

    /// Returns the secondary child expression (".expression" in sqlglot).
    #[getter]
    fn expression(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let child = slf.borrow().inner.select(Expression::get_expression);
        child
            .map(|child| {
                wrap_shared_expression(py, child, Some(slf.unbind().into_any()), Some("expression"))
            })
            .transpose()
    }

    /// Returns the list children (".expressions" in sqlglot).
    #[getter]
    fn expressions(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let children = slf
            .borrow()
            .inner
            .select_many(|root| root.get_expressions().iter().collect());
        let parent = slf.unbind().into_any();
        children
            .into_iter()
            .map(|child| {
                wrap_shared_expression(py, child, Some(parent.clone_ref(py)), Some("expressions"))
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
        matches!(&*self.inner, Expression::Literal(lit) if lit.is_string())
    }

    #[getter]
    fn is_number(&self) -> bool {
        match &*self.inner {
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
        match &*self.inner {
            Expression::Literal(lit) => check_int(lit),
            Expression::Neg(u) => {
                matches!(&u.this, Expression::Literal(lit) if check_int(lit))
            }
            _ => false,
        }
    }

    #[getter]
    fn is_star(&self) -> bool {
        match &*self.inner {
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
        let children = slf.borrow().inner.select_many(|root| root.children());
        let parent = slf.unbind().into_any();
        children
            .into_iter()
            .map(|child| {
                wrap_shared_expression(py, child, Some(parent.clone_ref(py)), Some("child"))
            })
            .collect()
    }

    #[pyo3(signature = (order = "dfs"))]
    fn walk(slf: Bound<'_, Self>, py: Python<'_>, order: &str) -> PyResult<Vec<Py<PyAny>>> {
        let inner = &slf.borrow().inner;
        let nodes = match order.trim().to_ascii_lowercase().as_str() {
            "dfs" => inner.select_many(|root| root.dfs().collect()),
            "bfs" => inner.select_many(|root| root.bfs().collect()),
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Unsupported walk order: {order}"
                )))
            }
        };
        // Walk results retain their existing parent-free behavior.
        nodes
            .into_iter()
            .map(|node| wrap_shared_expression(py, node, None, None))
            .collect()
    }

    #[pyo3(signature = (*types))]
    fn find(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        types: &Bound<'_, PyTuple>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let kinds = extract_kinds(types)?;
        let found = slf.borrow().inner.select(|root| {
            root.dfs()
                .skip(1)
                .find(|expr| kinds.iter().any(|kind| kind == expr.variant_name()))
        });
        found
            .map(|node| wrap_shared_expression(py, node, None, None))
            .transpose()
    }

    #[pyo3(signature = (*types))]
    fn find_all(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        types: &Bound<'_, PyTuple>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let kinds = extract_kinds(types)?;
        let nodes = slf.borrow().inner.select_many(|root| {
            root.dfs()
                .skip(1)
                .filter(|expr| kinds.iter().any(|kind| kind == expr.variant_name()))
                .collect()
        });
        nodes
            .into_iter()
            .map(|node| wrap_shared_expression(py, node, None, None))
            .collect()
    }

    /// Unwrap nested Paren expressions.
    fn unnest(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let child = slf.borrow().inner.select(|root| {
            let mut current = root;
            while let Expression::Paren(paren) = current {
                current = &paren.this;
            }
            (!std::ptr::eq(root, current)).then_some(current)
        });
        match child {
            Some(child) => wrap_shared_expression(py, child, None, None),
            None => Ok(slf.unbind().into_any()),
        }
    }

    /// Unwrap Alias expression.
    fn unalias(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let child = slf.borrow().inner.select(|root| match root {
            Expression::Alias(alias) => Some(&alias.this),
            _ => None,
        });
        match child {
            Some(child) => wrap_shared_expression(py, child, None, None),
            None => Ok(slf.unbind().into_any()),
        }
    }

    /// Flatten same-variant chains (e.g., And(And(a,b),c) → [a,b,c]).
    fn flatten(&self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        let nodes = self.inner.select_many(|root| {
            let mut result = Vec::new();
            let mut stack = vec![root];
            while let Some(expr) = stack.pop() {
                if expr.variant_name() == root.variant_name() {
                    if let Some(right) = expr.get_expression() {
                        stack.push(right);
                    }
                    if let Some(left) = expr.get_this() {
                        stack.push(left);
                    }
                } else {
                    result.push(expr);
                }
            }
            result
        });
        nodes
            .into_iter()
            .map(|node| wrap_shared_expression(py, node, None, None))
            .collect()
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
                        if let Ok(expr) = ast_json::expression_from_value(value.clone()) {
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

    // === SQLGlot-compatible builder methods ===

    fn eq(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "eq", other, false)
    }

    fn neq(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "neq", other, false)
    }

    fn lt(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "lt", other, false)
    }

    fn lte(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "lte", other, false)
    }

    fn gt(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "gt", other, false)
    }

    fn gte(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "gte", other, false)
    }

    fn and_(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "and", other, true)
    }

    fn or_(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "or", other, true)
    }

    fn xor(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "xor", other, true)
    }

    fn not_(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.builder_unary(py, "not")
    }

    fn is_(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "is", other, false)
    }

    fn is_null(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.builder_unary(py, "is_null")
    }

    fn is_not_null(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.builder_unary(py, "is_not_null")
    }

    fn like(&self, py: Python<'_>, pattern: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "like", pattern, false)
    }

    fn ilike(&self, py: Python<'_>, pattern: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "ilike", pattern, false)
    }

    fn rlike(&self, py: Python<'_>, pattern: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "rlike", pattern, false)
    }

    fn between(
        &self,
        py: Python<'_>,
        low: &Bound<'_, PyAny>,
        high: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let node = BuildNode::Between {
            expression: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
            low: Box::new(node_from_any(low, false)?),
            high: Box::new(node_from_any(high, false)?),
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    #[pyo3(signature = (*expressions))]
    fn isin(&self, py: Python<'_>, expressions: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
        let node = BuildNode::InList {
            expression: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
            values: nodes_from_tuple(expressions, false)?,
            negated: false,
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    #[pyo3(signature = (*expressions))]
    fn not_in(&self, py: Python<'_>, expressions: &Bound<'_, PyTuple>) -> PyResult<Py<PyAny>> {
        let node = BuildNode::InList {
            expression: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
            values: nodes_from_tuple(expressions, false)?,
            negated: true,
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    fn as_(&self, py: Python<'_>, alias: &str) -> PyResult<Py<PyAny>> {
        let node = BuildNode::Alias {
            expression: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
            alias: alias.to_string(),
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    fn cast(&self, py: Python<'_>, to: &str) -> PyResult<Py<PyAny>> {
        let node = BuildNode::Cast {
            expression: Box::new(BuildNode::Ast {
                expression: Box::new(self.inner.clone()),
            }),
            to: to.to_string(),
        };
        let expression = polyglot_sql::builder::plan::evaluate(&BuilderPlan::new(node), "generic")
            .map_err(crate::errors::map_parse_error)?;
        wrap_expression(py, expression)
    }

    fn asc(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.builder_ordered(py, false)
    }

    fn desc(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.builder_ordered(py, true)
    }

    #[pyo3(signature = (*expressions, append = true, dialect = None, copy = true))]
    fn select(
        &self,
        py: Python<'_>,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Select {
                expressions: nodes_from_tuple(expressions, true)?,
                append,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (expression, dialect = None, copy = true))]
    fn from_(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::From {
                source: node_from_any(expression, true)?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (expression, on = None, join_type = "", dialect = None, copy = true))]
    fn join(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        on: Option<&Bound<'_, PyAny>>,
        join_type: &str,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Join {
                source: node_from_any(expression, true)?,
                on: on.map(|value| node_from_any(value, true)).transpose()?,
                join_type: parse_join_type(join_type)?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(name = "where", signature = (*expressions, append = true, dialect = None, copy = true))]
    fn where_builder(
        &self,
        py: Python<'_>,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        self.builder_list_operation(py, "where", expressions, append, dialect, copy)
    }

    #[pyo3(signature = (*expressions, append = true, dialect = None, copy = true))]
    fn group_by(
        &self,
        py: Python<'_>,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        self.builder_list_operation(py, "group_by", expressions, append, dialect, copy)
    }

    #[pyo3(signature = (*expressions, append = true, dialect = None, copy = true))]
    fn having(
        &self,
        py: Python<'_>,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        self.builder_list_operation(py, "having", expressions, append, dialect, copy)
    }

    #[pyo3(signature = (*expressions, append = true, dialect = None, copy = true))]
    fn order_by(
        &self,
        py: Python<'_>,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        self.builder_list_operation(py, "order_by", expressions, append, dialect, copy)
    }

    #[pyo3(signature = (*expressions, append = true, dialect = None, copy = true))]
    fn sort_by(
        &self,
        py: Python<'_>,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        self.builder_list_operation(py, "sort_by", expressions, append, dialect, copy)
    }

    #[pyo3(signature = (expression, dialect = None, copy = true))]
    fn limit(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Limit {
                expression: node_from_any(expression, false)?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (expression, dialect = None, copy = true))]
    fn offset(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Offset {
                expression: node_from_any(expression, false)?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (distinct = true, copy = true))]
    fn distinct(&self, py: Python<'_>, distinct: bool, copy: bool) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Distinct { enabled: distinct },
            None,
            copy,
        )
    }

    #[pyo3(signature = (*expressions, append = true, dialect = None, copy = true))]
    fn qualify(
        &self,
        py: Python<'_>,
        expressions: &Bound<'_, PyTuple>,
        append: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        self.builder_list_operation(py, "qualify", expressions, append, dialect, copy)
    }

    #[pyo3(signature = (expression, table_alias = None, column_aliases = None, outer = false, dialect = None, copy = true))]
    #[allow(clippy::too_many_arguments)]
    fn lateral_view(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        table_alias: Option<&str>,
        column_aliases: Option<Vec<String>>,
        outer: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::LateralView {
                expression: node_from_any(expression, true)?,
                table_alias: table_alias.map(str::to_string),
                column_aliases: column_aliases.unwrap_or_default(),
                outer,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (name, partition_by = None, order_by = None, dialect = None, copy = true))]
    fn window(
        &self,
        py: Python<'_>,
        name: &str,
        partition_by: Option<&Bound<'_, PyTuple>>,
        order_by: Option<&Bound<'_, PyTuple>>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Window {
                name: name.to_string(),
                partition_by: partition_by
                    .map(|values| nodes_from_tuple(values, true))
                    .transpose()?
                    .unwrap_or_default(),
                order_by: order_by
                    .map(|values| nodes_from_tuple(values, true))
                    .transpose()?
                    .unwrap_or_default(),
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (copy = true))]
    fn for_update(&self, py: Python<'_>, copy: bool) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Lock {
                lock_type: polyglot_sql::builder::plan::LockType::Update,
            },
            None,
            copy,
        )
    }

    #[pyo3(signature = (copy = true))]
    fn for_share(&self, py: Python<'_>, copy: bool) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Lock {
                lock_type: polyglot_sql::builder::plan::LockType::Share,
            },
            None,
            copy,
        )
    }

    #[pyo3(signature = (text, copy = true))]
    fn hint(&self, py: Python<'_>, text: &str, copy: bool) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Hint {
                text: text.to_string(),
            },
            None,
            copy,
        )
    }

    #[pyo3(signature = (table, replace = false, temporary = false, copy = true))]
    fn ctas(
        &self,
        py: Python<'_>,
        table: &str,
        replace: bool,
        temporary: bool,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Ctas {
                table: table.to_string(),
                replace,
                temporary,
            },
            None,
            copy,
        )
    }

    #[pyo3(signature = (alias = None, copy = true))]
    fn subquery(&self, py: Python<'_>, alias: Option<&str>, copy: bool) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Subquery {
                alias: alias.map(str::to_string),
            },
            None,
            copy,
        )
    }

    #[pyo3(signature = (expression, distinct = true, dialect = None, copy = true))]
    fn union(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        distinct: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Union {
                other: node_from_any(expression, true)?,
                distinct,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (expression, distinct = true, dialect = None, copy = true))]
    fn intersect(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        distinct: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Intersect {
                other: node_from_any(expression, true)?,
                distinct,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (expression, distinct = true, dialect = None, copy = true))]
    fn except_(
        &self,
        py: Python<'_>,
        expression: &Bound<'_, PyAny>,
        distinct: bool,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Except {
                other: node_from_any(expression, true)?,
                distinct,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (condition, result, dialect = None, copy = true))]
    fn when(
        &self,
        py: Python<'_>,
        condition: &Bound<'_, PyAny>,
        result: &Bound<'_, PyAny>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::When {
                condition: node_from_any(condition, true)?,
                result: node_from_any(result, true)?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (result, dialect = None, copy = true))]
    fn else_(
        &self,
        py: Python<'_>,
        result: &Bound<'_, PyAny>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Else {
                result: node_from_any(result, true)?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (assignments, copy = true))]
    fn set_(
        &self,
        py: Python<'_>,
        assignments: &Bound<'_, PyDict>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        let assignments = assignments
            .iter()
            .map(|(key, value)| {
                Ok(BuilderAssignment {
                    column: key.extract::<String>()?,
                    value: node_from_any(&value, false)?,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        apply_and_wrap(py, self, BuildOperation::Set { assignments }, None, copy)
    }

    #[pyo3(signature = (columns, copy = true))]
    fn columns(&self, py: Python<'_>, columns: Vec<String>, copy: bool) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::InsertColumns { columns },
            None,
            copy,
        )
    }

    #[pyo3(signature = (*values, copy = true))]
    fn values(
        &self,
        py: Python<'_>,
        values: &Bound<'_, PyTuple>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Values {
                rows: vec![nodes_from_tuple(values, false)?],
                append: true,
            },
            None,
            copy,
        )
    }

    #[pyo3(signature = (query, copy = true))]
    fn query(&self, py: Python<'_>, query: &Bound<'_, PyAny>, copy: bool) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::Query {
                query: node_from_any(query, true)?,
            },
            None,
            copy,
        )
    }

    #[pyo3(signature = (source, on, dialect = None, copy = true))]
    fn merge_using(
        &self,
        py: Python<'_>,
        source: &Bound<'_, PyAny>,
        on: &Bound<'_, PyAny>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::MergeUsing {
                source: node_from_any(source, true)?,
                on: node_from_any(on, true)?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (assignments, condition = None, dialect = None, copy = true))]
    fn when_matched_update(
        &self,
        py: Python<'_>,
        assignments: &Bound<'_, PyDict>,
        condition: Option<&Bound<'_, PyAny>>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        let assignments = assignments
            .iter()
            .map(|(key, value)| {
                Ok(BuilderAssignment {
                    column: key.extract::<String>()?,
                    value: node_from_any(&value, false)?,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        apply_and_wrap(
            py,
            self,
            BuildOperation::WhenMatchedUpdate {
                assignments,
                condition: condition
                    .map(|value| node_from_any(value, true))
                    .transpose()?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (condition = None, dialect = None, copy = true))]
    fn when_matched_delete(
        &self,
        py: Python<'_>,
        condition: Option<&Bound<'_, PyAny>>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::WhenMatchedDelete {
                condition: condition
                    .map(|value| node_from_any(value, true))
                    .transpose()?,
            },
            dialect,
            copy,
        )
    }

    #[pyo3(signature = (columns, values, condition = None, dialect = None, copy = true))]
    fn when_not_matched_insert(
        &self,
        py: Python<'_>,
        columns: Vec<String>,
        values: &Bound<'_, PyTuple>,
        condition: Option<&Bound<'_, PyAny>>,
        dialect: Option<&str>,
        copy: bool,
    ) -> PyResult<Py<PyAny>> {
        apply_and_wrap(
            py,
            self,
            BuildOperation::WhenNotMatchedInsert {
                columns,
                values: nodes_from_tuple(values, false)?,
                condition: condition
                    .map(|value| node_from_any(value, true))
                    .transpose()?,
            },
            dialect,
            copy,
        )
    }

    fn __add__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "add", other, false)
    }
    fn __sub__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "sub", other, false)
    }
    fn __mul__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "mul", other, false)
    }
    fn __truediv__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "div", other, false)
    }
    fn __mod__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "mod", other, false)
    }
    fn __and__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "and", other, false)
    }
    fn __or__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "or", other, false)
    }
    fn __xor__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.builder_binary(py, "xor", other, false)
    }
    fn __invert__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.builder_unary(py, "not")
    }
    fn __neg__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.builder_unary(py, "neg")
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
