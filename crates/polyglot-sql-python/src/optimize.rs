use crate::errors::map_transpile_error;
use crate::helpers::{join_sql, resolve_read_or_dialect};
use polyglot_sql::optimizer::{optimize as core_optimize, OptimizerConfig};
use pyo3::prelude::*;

#[pyfunction(signature = (sql, dialect = None, *, read = None))]
pub fn optimize(
    py: Python<'_>,
    sql: &str,
    dialect: Option<&str>,
    read: Option<&str>,
) -> PyResult<String> {
    let dialect = resolve_read_or_dialect(read, dialect)?;

    let optimized = py.detach(|| {
        let expressions = dialect.parse(sql)?;
        let config = OptimizerConfig {
            dialect: Some(dialect.dialect_type()),
            ..Default::default()
        };

        let mut statements = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let optimized = core_optimize(expression, &config);
            statements.push(dialect.generate(&optimized)?);
        }

        Ok::<Vec<String>, polyglot_sql::Error>(statements)
    });

    optimized
        .map(|statements| join_sql(&statements))
        .map_err(map_transpile_error)
}
