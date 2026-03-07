use crate::errors::{map_generate_error, GenerateError};
use crate::helpers::{resolve_dialect, to_python_object};
use polyglot_sql::traversal::ExpressionWalk;
use polyglot_sql::Expression;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use serde_json::Value;

#[pyclass(skip_from_py_object, module = "polyglot_sql", name = "Expression")]
#[derive(Clone, Debug)]
pub struct PyExpression {
    pub(crate) inner: Expression,
}

impl PyExpression {
    pub fn from_inner(inner: Expression) -> Self {
        Self { inner }
    }
}

fn expression_value(expr: &Expression) -> PyResult<Value> {
    serde_json::to_value(expr)
        .map_err(|err| GenerateError::new_err(format!("Failed to serialize expression: {err}")))
}

fn expression_kind(expr: &Expression) -> PyResult<String> {
    match expression_value(expr)? {
        Value::Object(map) => map
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| GenerateError::new_err("Serialized expression had no variant key")),
        _ => Err(GenerateError::new_err(
            "Serialized expression was not an object",
        )),
    }
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

fn value_to_python_object(py: Python<'_>, value: Value) -> PyResult<Py<PyAny>> {
    if let Ok(expr) = serde_json::from_value::<Expression>(value.clone()) {
        return Py::new(py, PyExpression::from_inner(expr)).map(|expr| expr.into_any());
    }

    if let Ok(expressions) = serde_json::from_value::<Vec<Expression>>(value.clone()) {
        let wrapped = expressions
            .into_iter()
            .map(PyExpression::from_inner)
            .collect::<Vec<_>>();
        let objects = wrapped
            .into_iter()
            .map(|expr| Py::new(py, expr).map(|expr| expr.into_any()))
            .collect::<PyResult<Vec<_>>>()?;
        return Ok(pyo3::types::PyList::new(py, &objects)?.unbind().into_any());
    }

    to_python_object(py, &value)
}

#[pymethods]
impl PyExpression {
    #[getter]
    fn kind(&self) -> PyResult<String> {
        expression_kind(&self.inner)
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

    fn children(&self) -> Vec<PyExpression> {
        self.inner
            .children()
            .into_iter()
            .cloned()
            .map(PyExpression::from_inner)
            .collect()
    }

    #[pyo3(signature = (order = "dfs"))]
    fn walk(&self, order: &str) -> PyResult<Vec<PyExpression>> {
        let order = order.trim().to_ascii_lowercase();
        match order.as_str() {
            "dfs" => Ok(self
                .inner
                .dfs()
                .cloned()
                .map(PyExpression::from_inner)
                .collect()),
            "bfs" => Ok(self
                .inner
                .bfs()
                .cloned()
                .map(PyExpression::from_inner)
                .collect()),
            _ => Err(PyValueError::new_err(format!(
                "Unsupported walk order: {order}"
            ))),
        }
    }

    fn find(&self, kind: &str) -> PyResult<Option<PyExpression>> {
        let kind = normalize_kind(kind);
        Ok(self
            .inner
            .find(|expr| expression_kind(expr).is_ok_and(|value| value == kind))
            .cloned()
            .map(PyExpression::from_inner))
    }

    fn find_all(&self, kind: &str) -> PyResult<Vec<PyExpression>> {
        let kind = normalize_kind(kind);
        Ok(self
            .inner
            .find_all(|expr| expression_kind(expr).is_ok_and(|value| value == kind))
            .into_iter()
            .cloned()
            .map(PyExpression::from_inner)
            .collect())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Expression(kind={:?})", self.kind()?))
    }
}
