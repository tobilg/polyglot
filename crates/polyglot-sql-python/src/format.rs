use crate::errors::map_transpile_error;
use crate::helpers::{join_sql, resolve_dialect};
use polyglot_sql::dialects::Dialect;
use pyo3::prelude::*;

#[pyfunction(signature = (sql, dialect = "generic"))]
pub fn format_sql(py: Python<'_>, sql: &str, dialect: &str) -> PyResult<String> {
    resolve_dialect(dialect)?;

    let formatted = py.detach(|| {
        let dialect = Dialect::get_by_name(dialect)
            .expect("dialect existence checked before entering detach");
        let expressions = dialect.parse(sql)?;
        expressions
            .into_iter()
            .map(|expression| dialect.generate_pretty(&expression))
            .collect::<polyglot_sql::Result<Vec<String>>>()
    });

    formatted
        .map(|statements| join_sql(&statements))
        .map_err(map_transpile_error)
}
