use crate::errors::map_generate_error;
use crate::helpers::{ast_input_to_expressions, resolve_dialect};
use polyglot_sql::dialects::Dialect;
use pyo3::prelude::*;
use pyo3::types::PyAny;

#[pyfunction(signature = (ast, dialect = "generic", *, pretty = false))]
pub fn generate(
    py: Python<'_>,
    ast: &Bound<'_, PyAny>,
    dialect: &str,
    pretty: bool,
) -> PyResult<Vec<String>> {
    resolve_dialect(dialect)?;
    let expressions = ast_input_to_expressions(ast)?;

    py.detach(|| {
        let dialect = Dialect::get_by_name(dialect)
            .expect("dialect existence checked before entering detach");

        expressions
            .into_iter()
            .map(|expression| {
                if pretty {
                    dialect.generate_pretty(&expression)
                } else {
                    dialect.generate(&expression)
                }
            })
            .collect::<polyglot_sql::Result<Vec<String>>>()
    })
    .map_err(map_generate_error)
}
