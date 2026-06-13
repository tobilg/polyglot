use crate::helpers::{err_result, ok_json_result, panic_result, required_arg};
use crate::types::{PolyglotResult, STATUS_INVALID_ARGUMENT, STATUS_SERIALIZATION_ERROR};
use polyglot_sql::{ast_json, Expression, QualifyTablesOptions, RenameTablesOptions};
use serde::Deserialize;
use std::collections::HashMap;
use std::os::raw::c_char;

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct FfiQualifyTablesOptions {
    #[serde(default)]
    db: Option<String>,
    #[serde(default)]
    catalog: Option<String>,
    #[serde(default)]
    dialect: Option<String>,
    #[serde(default, alias = "canonicalize_table_aliases")]
    canonicalize_table_aliases: Option<bool>,
    #[serde(default, alias = "alias_unaliased_tables")]
    alias_unaliased_tables: Option<bool>,
    #[serde(default, alias = "alias_unaliased_subqueries")]
    alias_unaliased_subqueries: Option<bool>,
    #[serde(default, alias = "alias_prefix")]
    alias_prefix: Option<String>,
    #[serde(default, alias = "normalize_set_operation_subqueries")]
    normalize_set_operation_subqueries: Option<bool>,
}

impl FfiQualifyTablesOptions {
    fn into_core(self) -> Result<QualifyTablesOptions, PolyglotResult> {
        let dialect = match self
            .dialect
            .as_deref()
            .filter(|dialect| !dialect.is_empty())
            .map(str::parse)
            .transpose()
        {
            Ok(dialect) => dialect,
            Err(error) => {
                return Err(err_result(
                    STATUS_INVALID_ARGUMENT,
                    format!("Invalid qualify tables dialect: {error}"),
                ))
            }
        };

        let mut options = QualifyTablesOptions::new();
        options.db = self.db;
        options.catalog = self.catalog;
        options.dialect = dialect;
        if let Some(value) = self.canonicalize_table_aliases {
            options.canonicalize_table_aliases = value;
        }
        if let Some(value) = self.alias_unaliased_tables {
            options.alias_unaliased_tables = value;
        }
        if let Some(value) = self.alias_unaliased_subqueries {
            options.alias_unaliased_subqueries = value;
        }
        if let Some(value) = self.alias_prefix {
            options.alias_prefix = if value.is_empty() {
                "_".to_string()
            } else {
                value
            };
        }
        if let Some(value) = self.normalize_set_operation_subqueries {
            options.normalize_set_operation_subqueries = value;
        }
        Ok(options)
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct FfiRenameTablesOptions {
    #[serde(default, alias = "alias_renamed_tables")]
    alias_renamed_tables: Option<bool>,
    #[serde(default, alias = "preserve_existing_aliases")]
    preserve_existing_aliases: Option<bool>,
}

impl FfiRenameTablesOptions {
    fn into_core(self) -> RenameTablesOptions {
        let mut options = RenameTablesOptions::new();
        if let Some(value) = self.alias_renamed_tables {
            options.alias_renamed_tables = value;
        }
        if let Some(value) = self.preserve_existing_aliases {
            options.preserve_existing_aliases = value;
        }
        options
    }
}

/// Qualify table references in AST JSON.
///
/// `ast_json` must encode `Vec<Expression>`, matching `polyglot_parse` output.
#[no_mangle]
pub extern "C" fn polyglot_qualify_tables(
    ast_json: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| qualify_tables_impl(ast_json, options_json)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn qualify_tables_impl(ast_json: *const c_char, options_json: *const c_char) -> PolyglotResult {
    let ast_json = match unsafe { required_arg(ast_json, "ast_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let options_json = match unsafe { required_arg(options_json, "options_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let expressions: Vec<Expression> = match ast_json::expressions_from_str(&ast_json) {
        Ok(expressions) => expressions,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid AST JSON: {error}"),
            )
        }
    };
    let options = match serde_json::from_str::<FfiQualifyTablesOptions>(&options_json) {
        Ok(options) => match options.into_core() {
            Ok(options) => options,
            Err(result) => return result,
        },
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid qualify tables options JSON: {error}"),
            )
        }
    };

    let qualified: Vec<Expression> = expressions
        .into_iter()
        .map(|expression| polyglot_sql::qualify_tables(expression, &options))
        .collect();
    ok_json_result(&qualified)
}

/// Set LIMIT on AST JSON.
///
/// `ast_json` must encode `Vec<Expression>`, matching `polyglot_parse` output.
#[no_mangle]
pub extern "C" fn polyglot_set_limit(ast_json: *const c_char, limit: u64) -> PolyglotResult {
    match std::panic::catch_unwind(|| set_limit_impl(ast_json, limit)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn set_limit_impl(ast_json: *const c_char, limit: u64) -> PolyglotResult {
    let ast_json = match unsafe { required_arg(ast_json, "ast_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let expressions: Vec<Expression> = match ast_json::expressions_from_str(&ast_json) {
        Ok(expressions) => expressions,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid AST JSON: {error}"),
            )
        }
    };

    let transformed: Vec<Expression> = expressions
        .into_iter()
        .map(|expression| polyglot_sql::set_limit(expression, limit as usize))
        .collect();
    ok_json_result(&transformed)
}

/// Set OFFSET on AST JSON.
///
/// `ast_json` must encode `Vec<Expression>`, matching `polyglot_parse` output.
#[no_mangle]
pub extern "C" fn polyglot_set_offset(ast_json: *const c_char, offset: u64) -> PolyglotResult {
    match std::panic::catch_unwind(|| set_offset_impl(ast_json, offset)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn set_offset_impl(ast_json: *const c_char, offset: u64) -> PolyglotResult {
    let ast_json = match unsafe { required_arg(ast_json, "ast_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let expressions: Vec<Expression> = match ast_json::expressions_from_str(&ast_json) {
        Ok(expressions) => expressions,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid AST JSON: {error}"),
            )
        }
    };

    let transformed: Vec<Expression> = expressions
        .into_iter()
        .map(|expression| polyglot_sql::set_offset(expression, offset as usize))
        .collect();
    ok_json_result(&transformed)
}

/// Set ORDER BY on AST JSON.
///
/// `ast_json` and `order_by_json` must encode `Vec<Expression>`.
#[no_mangle]
pub extern "C" fn polyglot_set_order_by(
    ast_json: *const c_char,
    order_by_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| set_order_by_impl(ast_json, order_by_json)) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn set_order_by_impl(ast_json: *const c_char, order_by_json: *const c_char) -> PolyglotResult {
    let ast_json = match unsafe { required_arg(ast_json, "ast_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let order_by_json = match unsafe { required_arg(order_by_json, "order_by_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let expressions: Vec<Expression> = match ast_json::expressions_from_str(&ast_json) {
        Ok(expressions) => expressions,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid AST JSON: {error}"),
            )
        }
    };
    let order_by: Vec<Expression> = match ast_json::expressions_from_str(&order_by_json) {
        Ok(expressions) => expressions,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid order_by JSON: {error}"),
            )
        }
    };

    let transformed: Vec<Expression> = expressions
        .into_iter()
        .map(|expression| polyglot_sql::set_order_by(expression, order_by.clone()))
        .collect();
    ok_json_result(&transformed)
}

/// Rename tables in AST JSON with options.
///
/// `ast_json` must encode `Vec<Expression>`, matching `polyglot_parse` output.
#[no_mangle]
pub extern "C" fn polyglot_rename_tables_with_options(
    ast_json: *const c_char,
    mapping_json: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    match std::panic::catch_unwind(|| {
        rename_tables_with_options_impl(ast_json, mapping_json, options_json)
    }) {
        Ok(result) => result,
        Err(panic) => panic_result(panic),
    }
}

fn rename_tables_with_options_impl(
    ast_json: *const c_char,
    mapping_json: *const c_char,
    options_json: *const c_char,
) -> PolyglotResult {
    let ast_json = match unsafe { required_arg(ast_json, "ast_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let mapping_json = match unsafe { required_arg(mapping_json, "mapping_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };
    let options_json = match unsafe { required_arg(options_json, "options_json") } {
        Ok(value) => value,
        Err(result) => return result,
    };

    let expressions: Vec<Expression> = match ast_json::expressions_from_str(&ast_json) {
        Ok(expressions) => expressions,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid AST JSON: {error}"),
            )
        }
    };
    let mapping: HashMap<String, String> = match serde_json::from_str(&mapping_json) {
        Ok(mapping) => mapping,
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid table mapping JSON: {error}"),
            )
        }
    };
    let options = match serde_json::from_str::<FfiRenameTablesOptions>(&options_json) {
        Ok(options) => options.into_core(),
        Err(error) => {
            return err_result(
                STATUS_SERIALIZATION_ERROR,
                format!("Invalid rename tables options JSON: {error}"),
            )
        }
    };

    let renamed: Vec<Expression> = expressions
        .into_iter()
        .map(|expression| polyglot_sql::rename_tables_with_options(expression, &mapping, &options))
        .collect();
    ok_json_result(&renamed)
}
