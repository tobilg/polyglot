use crate::errors::map_transpile_error;
use crate::helpers::{join_sql, resolve_dialect};
use polyglot_sql::FormatGuardOptions;
use pyo3::prelude::*;

#[pyfunction(signature = (sql, dialect = "generic", *, max_input_bytes = None, max_tokens = None, max_ast_nodes = None, max_set_op_chain = None))]
pub fn format_sql(
    py: Python<'_>,
    sql: &str,
    dialect: &str,
    max_input_bytes: Option<usize>,
    max_tokens: Option<usize>,
    max_ast_nodes: Option<usize>,
    max_set_op_chain: Option<usize>,
) -> PyResult<String> {
    resolve_dialect(dialect)?;

    let mut options = FormatGuardOptions::default();
    if let Some(value) = max_input_bytes {
        options.max_input_bytes = Some(value);
    }
    if let Some(value) = max_tokens {
        options.max_tokens = Some(value);
    }
    if let Some(value) = max_ast_nodes {
        options.max_ast_nodes = Some(value);
    }
    if let Some(value) = max_set_op_chain {
        options.max_set_op_chain = Some(value);
    }

    let formatted = py.detach(|| polyglot_sql::format_with_options_by_name(sql, dialect, &options));

    formatted
        .map(|statements| join_sql(&statements))
        .map_err(map_transpile_error)
}
