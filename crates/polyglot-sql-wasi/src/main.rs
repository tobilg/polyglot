//! A WASI command that exposes polyglot-sql over a JSON protocol on stdin and
//! stdout.
//!
//! The other bindings in this workspace each come with a runtime requirement.
//! The wasm-bindgen module needs a JavaScript host; the FFI library has to be
//! built per platform and then located at run time, which for a program that
//! itself ships as a single binary means shipping and finding a second native
//! artifact. A WASI command avoids both: a host embeds the `.wasm` and runs it
//! in a pure-Go runtime such as wazero, with no cgo, no dynamic loading, and
//! one artifact for every platform.
//!
//! The protocol is one request per run. The host writes a JSON object to
//! stdin, the module writes a JSON object to stdout and exits 0. Exiting after
//! a single request is what keeps the module free of any state that could
//! leak between calls, and instantiation is cheap enough to pay per call once
//! the host has compiled the module.
//!
//! Request:
//!
//! ```json
//! {"action": "parse",     "sql": "SELECT 1", "dialect": "oracle"}
//! {"action": "generate",  "ast": [ ... ],    "dialect": "oracle"}
//! {"action": "transpile", "sql": "SELECT 1", "read_dialect": "oracle", "write_dialect": "duckdb"}
//! {"action": "format",    "sql": "SELECT 1", "dialect": "oracle"}
//! ```
//!
//! An unrecognised dialect *name* is reported as `Unknown dialect: <name>`.
//! A name that exists but whose crate feature was not compiled in is not: it
//! falls through to the generic dialect and reports success. That is why this
//! module is built with every dialect rather than letting a host choose one.
//!
//! Response, where `result` is whatever the action returns:
//!
//! ```json
//! {"ok": true,  "result": ..., "error": null}
//! {"ok": false, "result": null, "error": "Parse error at line 1, column 8: ..."}
//! ```
//!
//! `generate` is the reason this exists rather than being a copy of an
//! existing shim: parsing to an AST is only half of a rewrite, and without a
//! way back to SQL a host can inspect a query but not modify one.

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Request {
    action: String,

    #[serde(default)]
    sql: Option<String>,

    /// An AST as returned by `parse`. Either a single expression or a list of
    /// them: `parse` returns a list, so accepting a list here means a host can
    /// feed back exactly what it was given.
    #[serde(default)]
    ast: Option<serde_json::Value>,

    #[serde(default)]
    dialect: Option<String>,

    #[serde(default)]
    read_dialect: Option<String>,

    #[serde(default)]
    write_dialect: Option<String>,
}

#[derive(Serialize)]
struct Response {
    ok: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,

    error: Option<String>,
}

impl Response {
    fn ok(result: serde_json::Value) -> Self {
        Response {
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    fn err(message: impl Into<String>) -> Self {
        Response {
            ok: false,
            result: None,
            error: Some(message.into()),
        }
    }
}

fn main() {
    let mut input = String::new();
    if let Err(e) = io::stdin().read_to_string(&mut input) {
        write(&Response::err(format!("Could not read stdin: {e}")));
        return;
    }

    let response = match serde_json::from_str::<Request>(&input) {
        Ok(request) => dispatch(request),
        Err(e) => Response::err(format!("Request is not valid JSON: {e}")),
    };
    write(&response);
}

/// Writes the response and nothing else. A malformed response would leave the
/// host unable to report anything useful, so serialisation failure falls back
/// to a hand-built envelope rather than panicking.
fn write(response: &Response) {
    let body = serde_json::to_string(response).unwrap_or_else(|e| {
        format!(
            r#"{{"ok":false,"error":"Could not serialise the response: {}"}}"#,
            e.to_string().replace('"', "'")
        )
    });
    let mut stdout = io::stdout();
    let _ = stdout.write_all(body.as_bytes());
    let _ = stdout.flush();
}

fn dispatch(request: Request) -> Response {
    match request.action.as_str() {
        "parse" => parse(request),
        "generate" => generate(request),
        "transpile" => transpile(request),
        "format" => format(request),
        other => Response::err(format!("unknown action: {other}")),
    }
}

/// Resolves a dialect name, defaulting to the generic dialect when absent.
fn dialect_of(name: Option<&String>) -> Result<polyglot_sql::DialectType, String> {
    match name {
        None => Ok(polyglot_sql::DialectType::Generic),
        Some(name) => name
            .parse::<polyglot_sql::DialectType>()
            .map_err(|_| format!("Unknown dialect: {name}")),
    }
}

fn require_sql(request: &Request) -> Result<&str, String> {
    request
        .sql
        .as_deref()
        .ok_or_else(|| "The request has no \"sql\" field".to_string())
}

fn parse(request: Request) -> Response {
    let dialect = match dialect_of(request.dialect.as_ref()) {
        Ok(d) => d,
        Err(e) => return Response::err(e),
    };
    let sql = match require_sql(&request) {
        Ok(sql) => sql,
        Err(e) => return Response::err(e),
    };

    match polyglot_sql::parse(sql, dialect) {
        Ok(ast) => match serde_json::to_value(&ast) {
            Ok(value) => Response::ok(value),
            Err(e) => Response::err(format!("Could not serialise the AST: {e}")),
        },
        Err(e) => Response::err(e.to_string()),
    }
}

/// Generates SQL from an AST, accepting either one expression or a list of
/// them so a host can pass back exactly what `parse` returned.
fn generate(request: Request) -> Response {
    let dialect = match dialect_of(request.dialect.as_ref()) {
        Ok(d) => d,
        Err(e) => return Response::err(e),
    };
    let Some(ast) = request.ast else {
        return Response::err("The request has no \"ast\" field");
    };

    let expressions: Vec<polyglot_sql::Expression> = if ast.is_array() {
        match serde_json::from_value(ast) {
            Ok(list) => list,
            Err(e) => return Response::err(format!("The AST list could not be read: {e}")),
        }
    } else {
        match serde_json::from_value(ast) {
            Ok(one) => vec![one],
            Err(e) => return Response::err(format!("The AST could not be read: {e}")),
        }
    };

    let mut out = Vec::with_capacity(expressions.len());
    for expression in &expressions {
        match polyglot_sql::generate(expression, dialect) {
            Ok(sql) => out.push(sql),
            Err(e) => return Response::err(e.to_string()),
        }
    }
    Response::ok(serde_json::Value::from(out))
}

fn transpile(request: Request) -> Response {
    let read = match dialect_of(request.read_dialect.as_ref()) {
        Ok(d) => d,
        Err(e) => return Response::err(e),
    };
    let write_dialect = match dialect_of(request.write_dialect.as_ref()) {
        Ok(d) => d,
        Err(e) => return Response::err(e),
    };
    let sql = match require_sql(&request) {
        Ok(sql) => sql,
        Err(e) => return Response::err(e),
    };

    match polyglot_sql::transpile(sql, read, write_dialect) {
        Ok(results) => Response::ok(serde_json::Value::from(results)),
        Err(e) => Response::err(e.to_string()),
    }
}

fn format(request: Request) -> Response {
    let dialect = match dialect_of(request.dialect.as_ref()) {
        Ok(d) => d,
        Err(e) => return Response::err(e),
    };
    let sql = match require_sql(&request) {
        Ok(sql) => sql,
        Err(e) => return Response::err(e),
    };

    match polyglot_sql::transpile(sql, dialect, dialect) {
        Ok(results) => Response::ok(serde_json::Value::from(results)),
        Err(e) => Response::err(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs one request through the dispatcher, which is the whole protocol
    /// minus stdin and stdout. These run on the host target, so the protocol
    /// is covered by `cargo test` without a wasm runtime.
    fn call(body: serde_json::Value) -> Response {
        let request: Request =
            serde_json::from_value(body).expect("the test request should deserialise");
        dispatch(request)
    }

    fn result_of(response: &Response) -> &serde_json::Value {
        response
            .result
            .as_ref()
            .unwrap_or_else(|| panic!("expected a result, got error {:?}", response.error))
    }

    #[test]
    fn parse_then_generate_round_trips() {
        let sql = "SELECT a FROM t WHERE b = 1";
        let parsed = call(serde_json::json!({
            "action": "parse", "sql": sql, "dialect": "oracle"
        }));
        assert!(parsed.ok, "parse failed: {:?}", parsed.error);

        // The AST goes back exactly as it came out, which is what a host that
        // modified it would be handing over.
        let generated = call(serde_json::json!({
            "action": "generate", "ast": result_of(&parsed), "dialect": "oracle"
        }));
        assert!(generated.ok, "generate failed: {:?}", generated.error);
        assert_eq!(result_of(&generated), &serde_json::json!([sql]));
    }

    /// A host rewrites a query by modifying the AST, not the text. This is the
    /// case that motivated adding `generate`: select a partition on the table
    /// reference, then ask for the SQL back.
    #[test]
    fn a_modified_ast_generates_sql() {
        let parsed = call(serde_json::json!({
            "action": "parse",
            "sql": "SELECT ID FROM SALES.ORDERS WHERE REGION = 'EU'",
            "dialect": "oracle"
        }));
        assert!(parsed.ok, "parse failed: {:?}", parsed.error);

        let mut ast = result_of(&parsed).clone();
        ast[0]["select"]["from"]["expressions"][0]["table"]["partitions"] =
            serde_json::json!([{"name": "P1", "quoted": false}]);

        let generated = call(serde_json::json!({
            "action": "generate", "ast": ast, "dialect": "oracle"
        }));
        assert!(generated.ok, "generate failed: {:?}", generated.error);
        assert_eq!(
            result_of(&generated),
            &serde_json::json!(["SELECT ID FROM SALES.ORDERS PARTITION(P1) WHERE REGION = 'EU'"])
        );
    }

    /// `parse` returns a list, so `generate` has to accept one. Accepting a
    /// bare expression too means a host that pulled one statement out of the
    /// list does not have to re-wrap it.
    #[test]
    fn generate_accepts_one_expression_or_a_list() {
        let parsed = call(serde_json::json!({
            "action": "parse", "sql": "SELECT 1 FROM DUAL", "dialect": "oracle"
        }));
        let list = result_of(&parsed).clone();
        let single = list[0].clone();

        for ast in [list, single] {
            let generated = call(serde_json::json!({
                "action": "generate", "ast": ast, "dialect": "oracle"
            }));
            assert!(generated.ok, "generate failed: {:?}", generated.error);
            assert_eq!(
                result_of(&generated),
                &serde_json::json!(["SELECT 1 FROM DUAL"])
            );
        }
    }

    #[test]
    fn transpile_and_format_work() {
        let transpiled = call(serde_json::json!({
            "action": "transpile", "sql": "SELECT a FROM t",
            "read_dialect": "oracle", "write_dialect": "duckdb"
        }));
        assert!(transpiled.ok, "transpile failed: {:?}", transpiled.error);

        let formatted = call(serde_json::json!({
            "action": "format", "sql": "select   a from t", "dialect": "oracle"
        }));
        assert!(formatted.ok, "format failed: {:?}", formatted.error);
        assert_eq!(
            result_of(&formatted),
            &serde_json::json!(["SELECT a FROM t"])
        );
    }

    /// Every way a request can be wrong has to come back as a reported error
    /// rather than a panic, because a panic in a wasm module built with
    /// panic=abort reaches the host as an opaque trap with no message.
    #[test]
    fn bad_requests_are_reported_not_fatal() {
        let cases = [
            (serde_json::json!({"action": "nope"}), "unknown action"),
            (
                serde_json::json!({"action": "parse", "dialect": "oracle"}),
                "no \"sql\" field",
            ),
            (
                serde_json::json!({"action": "generate", "dialect": "oracle"}),
                "no \"ast\" field",
            ),
            (
                serde_json::json!({"action": "parse", "sql": "SELECT 1", "dialect": "nope"}),
                "Unknown dialect",
            ),
            (
                serde_json::json!({"action": "parse", "sql": "SELECT FROM", "dialect": "oracle"}),
                "error",
            ),
            (
                serde_json::json!({"action": "generate", "ast": "not an ast", "dialect": "oracle"}),
                "could not be read",
            ),
        ];
        for (body, expected) in cases {
            let response = call(body.clone());
            assert!(!response.ok, "{body} was accepted");
            let error = response.error.unwrap_or_default();
            assert!(
                error.to_lowercase().contains(&expected.to_lowercase()),
                "{body} reported {error:?}, which does not mention {expected:?}"
            );
        }
    }

    /// An absent dialect means the generic one, so a host can omit it.
    #[test]
    fn a_missing_dialect_is_generic() {
        let response = call(serde_json::json!({"action": "parse", "sql": "SELECT 1"}));
        assert!(
            response.ok,
            "parse without a dialect failed: {:?}",
            response.error
        );
    }
}
