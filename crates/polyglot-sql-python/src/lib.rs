mod annotate_types;
mod dialects;
mod diff;
mod errors;
mod expr;
mod expr_types;
mod format;
mod generate;
mod helpers;
mod lineage;
mod openlineage;
mod optimize;
mod parse;
mod query_analysis;
mod tokenize;
mod transforms;
mod transpile;
mod types;
mod validate;

use pyo3::prelude::*;

#[pymodule]
fn _polyglot_sql(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(transpile::transpile, m)?)?;
    m.add_function(wrap_pyfunction!(parse::parse, m)?)?;
    m.add_function(wrap_pyfunction!(parse::parse_one, m)?)?;
    m.add_function(wrap_pyfunction!(parse::parse_data_type, m)?)?;
    m.add_function(wrap_pyfunction!(generate::generate, m)?)?;
    m.add_function(wrap_pyfunction!(format::format_sql, m)?)?;
    m.add_function(wrap_pyfunction!(validate::validate, m)?)?;
    m.add_function(wrap_pyfunction!(optimize::optimize, m)?)?;
    m.add_function(wrap_pyfunction!(lineage::lineage, m)?)?;
    m.add_function(wrap_pyfunction!(lineage::lineage_with_schema, m)?)?;
    m.add_function(wrap_pyfunction!(lineage::source_tables, m)?)?;
    m.add_function(wrap_pyfunction!(
        openlineage::openlineage_column_lineage,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(openlineage::openlineage_job_event, m)?)?;
    m.add_function(wrap_pyfunction!(openlineage::openlineage_run_event, m)?)?;
    m.add_function(wrap_pyfunction!(query_analysis::analyze_query, m)?)?;
    m.add_function(wrap_pyfunction!(diff::diff, m)?)?;
    m.add_function(wrap_pyfunction!(tokenize::tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(annotate_types::annotate_types, m)?)?;
    m.add_function(wrap_pyfunction!(transforms::qualify_tables, m)?)?;
    m.add_function(wrap_pyfunction!(transforms::set_limit, m)?)?;
    m.add_function(wrap_pyfunction!(transforms::set_offset, m)?)?;
    m.add_function(wrap_pyfunction!(transforms::set_order_by, m)?)?;
    m.add_function(wrap_pyfunction!(transforms::rename_tables, m)?)?;
    m.add_function(wrap_pyfunction!(dialects::dialects, m)?)?;
    m.add_function(wrap_pyfunction!(dialects::version, m)?)?;

    errors::register_exceptions(m)?;
    m.add_class::<expr::PyExpression>()?;
    expr_types::register_expression_subclasses(m)?;
    m.add_class::<types::ValidationResult>()?;
    m.add_class::<types::ValidationErrorInfo>()?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
