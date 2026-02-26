mod dialects;
mod diff;
mod errors;
mod format;
mod generate;
mod helpers;
mod lineage;
mod optimize;
mod parse;
mod tokenize;
mod transpile;
mod types;
mod validate;

use pyo3::prelude::*;

#[pymodule]
fn _polyglot_sql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(transpile::transpile, m)?)?;
    m.add_function(wrap_pyfunction!(parse::parse, m)?)?;
    m.add_function(wrap_pyfunction!(parse::parse_one, m)?)?;
    m.add_function(wrap_pyfunction!(generate::generate, m)?)?;
    m.add_function(wrap_pyfunction!(format::format_sql, m)?)?;
    m.add_function(wrap_pyfunction!(validate::validate, m)?)?;
    m.add_function(wrap_pyfunction!(optimize::optimize, m)?)?;
    m.add_function(wrap_pyfunction!(lineage::lineage, m)?)?;
    m.add_function(wrap_pyfunction!(lineage::source_tables, m)?)?;
    m.add_function(wrap_pyfunction!(diff::diff, m)?)?;
    m.add_function(wrap_pyfunction!(tokenize::tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(dialects::dialects, m)?)?;
    m.add_function(wrap_pyfunction!(dialects::version, m)?)?;

    errors::register_exceptions(m)?;
    m.add_class::<types::ValidationResult>()?;
    m.add_class::<types::ValidationErrorInfo>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
