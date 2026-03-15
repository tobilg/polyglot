use crate::errors::{parse_statement_count_error, unknown_dialect_error, GenerateError};
use crate::expr::PyExpression;
use polyglot_sql::dialects::Dialect;
use polyglot_sql::Expression;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyList};
use pythonize::{depythonize, pythonize};
use serde::Serialize;
use std::sync::{mpsc, Mutex, OnceLock};

/// Stack size for the persistent worker thread (64 MB).
const WORKER_STACK_SIZE: usize = 64 * 1024 * 1024;

type ParseTask = Box<dyn FnOnce() + Send>;

/// A persistent worker thread with a large stack.
struct Worker {
    sender: mpsc::Sender<ParseTask>,
}

impl Worker {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel::<ParseTask>();
        std::thread::Builder::new()
            .name("polyglot-worker".into())
            .stack_size(WORKER_STACK_SIZE)
            .spawn(move || {
                while let Ok(task) = rx.recv() {
                    task();
                }
            })
            .expect("failed to spawn polyglot worker thread");
        Self { sender: tx }
    }
}

static WORKER: OnceLock<Mutex<Worker>> = OnceLock::new();

fn get_worker() -> &'static Mutex<Worker> {
    WORKER.get_or_init(|| Mutex::new(Worker::new()))
}

/// Wrapper around `mpsc::Receiver` that implements `Send` + `Sync` so it
/// satisfies PyO3's `Ungil` bound inside `py.detach()`.
///
/// Safety: the receiver is only used by one thread (the calling thread inside
/// `detach`), so the `Sync` impl is sound — no concurrent access occurs.
struct UnsafeSyncReceiver<T>(mpsc::Receiver<T>);
unsafe impl<T: Send> Sync for UnsafeSyncReceiver<T> {}

/// Run a closure on a persistent thread with a 64 MB stack.
/// Releases the GIL while waiting for the result.
pub fn run_on_large_stack<T, F>(py: Python<'_>, f: F) -> PyResult<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let (result_tx, result_rx) = mpsc::channel();
    let result_rx = UnsafeSyncReceiver(result_rx);

    {
        let worker = get_worker()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("worker thread lock poisoned"))?;
        worker
            .sender
            .send(Box::new(move || {
                let result = f();
                let _ = result_tx.send(result);
            }))
            .map_err(|_| PyRuntimeError::new_err("worker thread has stopped"))?;
    }

    py.detach(move || {
        result_rx
            .0
            .recv()
            .map_err(|_| PyRuntimeError::new_err("worker thread panicked"))
    })
}

/// Parse SQL on a persistent thread with a large stack, returning the parsed expressions.
pub fn parse_on_large_stack(
    py: Python<'_>,
    dialect: &Dialect,
    sql: &str,
) -> PyResult<Vec<Expression>> {
    let dialect_type = dialect.dialect_type();
    let sql_owned = sql.to_owned();
    run_on_large_stack(py, move || {
        let d = Dialect::get(dialect_type);
        d.parse(&sql_owned)
    })?
    .map_err(crate::errors::map_parse_error)
}

pub fn resolve_dialect(name: &str) -> PyResult<Dialect> {
    Dialect::get_by_name(name).ok_or_else(|| unknown_dialect_error(name))
}

pub fn resolve_read_or_dialect(read: Option<&str>, dialect: Option<&str>) -> PyResult<Dialect> {
    resolve_dialect(read.or(dialect).unwrap_or("generic"))
}

pub fn normalize_error_level(error_level: Option<&str>) -> PyResult<Option<&str>> {
    match error_level.map(|level| level.to_ascii_lowercase()) {
        None => Ok(None),
        Some(level) if matches!(level.as_str(), "raise" | "immediate" | "warn" | "ignore") => {
            Ok(error_level)
        }
        Some(level) => Err(PyValueError::new_err(format!(
            "Unsupported error_level: {level}"
        ))),
    }
}

pub fn reject_parse_into(into: Option<&Bound<'_, PyAny>>) -> PyResult<()> {
    if into.is_some() {
        return Err(PyNotImplementedError::new_err(
            "parse_one(into=...) is not supported in polyglot_sql",
        ));
    }
    Ok(())
}

pub fn to_python_object<T>(py: Python<'_>, value: &T) -> PyResult<Py<PyAny>>
where
    T: Serialize,
{
    let py_value = pythonize(py, value).map_err(|err| {
        pyo3::exceptions::PyRuntimeError::new_err(format!("Python conversion error: {err}"))
    })?;
    Ok(py_value.unbind())
}

pub fn parse_single_statement(sql: &str, dialect: &Dialect) -> PyResult<Expression> {
    let mut expressions = dialect.parse(sql).map_err(crate::errors::map_parse_error)?;
    if expressions.len() != 1 {
        return Err(parse_statement_count_error(expressions.len()));
    }
    Ok(expressions.remove(0))
}

pub fn join_sql(statements: &[String]) -> String {
    if statements.is_empty() {
        String::new()
    } else if statements.len() == 1 {
        statements[0].clone()
    } else {
        statements.join(";\n")
    }
}

pub fn ast_input_to_expressions(ast: &Bound<'_, PyAny>) -> PyResult<Vec<Expression>> {
    if let Ok(expr) = ast.extract::<PyRef<'_, PyExpression>>() {
        return Ok(vec![expr.inner.clone()]);
    }

    if ast.cast::<PyDict>().is_ok() {
        let expr: Expression = depythonize(ast).map_err(|err| {
            GenerateError::new_err(format!("Failed to decode AST expression: {err}"))
        })?;
        return Ok(vec![expr]);
    }

    if ast.cast::<PyList>().is_ok() {
        let list = ast.cast::<PyList>().expect("cast checked above");
        let mut expressions = Vec::with_capacity(list.len());
        for item in list.iter() {
            if let Ok(expr) = item.extract::<PyRef<'_, PyExpression>>() {
                expressions.push(expr.inner.clone());
                continue;
            }

            let expr: Expression = depythonize(&item).map_err(|err| {
                GenerateError::new_err(format!("Failed to decode AST expression list item: {err}"))
            })?;
            expressions.push(expr);
        }
        return Ok(expressions);
    }

    Err(GenerateError::new_err(
        "AST must be an Expression, a dict (single expression), or a list of Expressions/dicts",
    ))
}
