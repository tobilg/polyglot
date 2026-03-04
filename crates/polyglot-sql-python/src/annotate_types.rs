use crate::helpers::{resolve_dialect, to_python_object};
use pyo3::prelude::*;
use pyo3::types::PyAny;
use pythonize::depythonize;

/// Parse SQL and annotate the AST with inferred type information.
///
/// Returns the parsed AST (list of expression dicts) with `inferred_type`
/// fields populated on value-producing nodes (columns, operators, functions,
/// casts, etc.).
///
/// Args:
///     sql: SQL string to parse and annotate
///     schema: Optional schema dict for column type resolution
///         (same format as `lineage_with_schema`)
///     dialect: Dialect for parsing (default: "generic")
///
/// Returns:
///     List of annotated AST expression dicts
///
/// Example:
///     >>> ast = annotate_types("SELECT 1 + 2.0")
///     >>> # The 'add' node will have inferred_type set to a float type
#[pyfunction(signature = (sql, schema = None, dialect = "generic"))]
pub fn annotate_types(
    py: Python<'_>,
    sql: &str,
    schema: Option<&Bound<'_, PyAny>>,
    dialect: &str,
) -> PyResult<Py<PyAny>> {
    resolve_dialect(dialect)?;

    let mapping_schema = if let Some(schema_obj) = schema {
        let validation_schema: polyglot_sql::ValidationSchema =
            depythonize(schema_obj).map_err(|err| {
                pyo3::exceptions::PyValueError::new_err(format!(
                    "Invalid schema object (expected ValidationSchema shape): {err}"
                ))
            })?;
        Some(polyglot_sql::mapping_schema_from_validation_schema(
            &validation_schema,
        ))
    } else {
        None
    };

    let expressions = py.detach(|| {
        let dialect_impl = polyglot_sql::dialects::Dialect::get_by_name(dialect)
            .expect("dialect existence checked before entering detach");
        let mut expressions = dialect_impl
            .parse(sql)
            .map_err(crate::errors::map_parse_error)?;

        let dialect_type = dialect_impl.dialect_type();
        let dialect_opt = if dialect_type == polyglot_sql::DialectType::Generic {
            None
        } else {
            Some(dialect_type)
        };

        for expr in &mut expressions {
            polyglot_sql::annotate_types(
                expr,
                mapping_schema
                    .as_ref()
                    .map(|s| s as &dyn polyglot_sql::Schema),
                dialect_opt,
            );
        }

        Ok::<Vec<polyglot_sql::Expression>, PyErr>(expressions)
    })?;

    to_python_object(py, &expressions)
}
