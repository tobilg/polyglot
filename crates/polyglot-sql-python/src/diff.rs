use crate::helpers::{parse_single_statement, resolve_dialect, to_python_object};
use polyglot_sql::diff::{diff as core_diff, Edit};
use polyglot_sql::Expression;
use pyo3::prelude::*;
use pyo3::types::PyAny;
use serde::Serialize;

#[derive(Debug, Serialize)]
struct DiffEntry {
    edit_type: String,
    expression: Expression,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<Expression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<Expression>,
}

#[pyfunction(signature = (sql1, sql2, dialect = "generic"))]
pub fn diff(py: Python<'_>, sql1: &str, sql2: &str, dialect: &str) -> PyResult<Py<PyAny>> {
    resolve_dialect(dialect)?;

    let entries = py.detach(|| {
        let dialect_impl = polyglot_sql::dialects::Dialect::get_by_name(dialect)
            .expect("dialect existence checked before entering detach");
        let source = parse_single_statement(sql1, &dialect_impl)?;
        let target = parse_single_statement(sql2, &dialect_impl)?;
        let edits = core_diff(&source, &target, true);
        Ok::<Vec<DiffEntry>, PyErr>(edits.into_iter().map(edit_to_entry).collect())
    })?;

    to_python_object(py, &entries)
}

fn edit_to_entry(edit: Edit) -> DiffEntry {
    match edit {
        Edit::Insert { expression } => DiffEntry {
            edit_type: "insert".to_string(),
            expression,
            source: None,
            target: None,
        },
        Edit::Remove { expression } => DiffEntry {
            edit_type: "remove".to_string(),
            expression,
            source: None,
            target: None,
        },
        Edit::Move { source, target } => DiffEntry {
            edit_type: "move".to_string(),
            expression: target.clone(),
            source: Some(source),
            target: Some(target),
        },
        Edit::Update { source, target } => DiffEntry {
            edit_type: "update".to_string(),
            expression: target.clone(),
            source: Some(source),
            target: Some(target),
        },
        Edit::Keep { source, target } => DiffEntry {
            edit_type: "keep".to_string(),
            expression: target.clone(),
            source: Some(source),
            target: Some(target),
        },
    }
}
