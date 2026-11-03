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
//! {"ok": false, "error": "Parse error at line 1, column 8: ..."}
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

    write(&decode_and_dispatch(&input));
}

/// Decodes a request and runs it.
///
/// serde_json's default nesting limit is 128, which is below what this
/// module's own parser produces: the AST for `SELECT a + a + ... + a FROM t`
/// with 61 terms is more than 128 JSON levels deep, so `generate` rejected an
/// AST that `parse` had just returned. The limit is therefore disabled and
/// replaced by an explicit depth check, which keeps a bound on recursion while
/// making the protocol able to carry whatever the parser emits.
///
/// The bound is checked before any AST is built, so an input beyond it gets a
/// JSON error naming the limit rather than exhausting the stack. Overflowing
/// the stack in WASM reaches the host as an opaque trap with no message, which
/// is the one outcome worth engineering away.
fn decode_and_dispatch(input: &str) -> Response {
    if let Some(depth) = json_depth_exceeding(input, MAX_REQUEST_JSON_DEPTH) {
        return Response::err(format!(
            "Request nests {depth} levels deep, past the supported limit of \
             {MAX_REQUEST_JSON_DEPTH}"
        ));
    }

    let mut deserializer = serde_json::Deserializer::from_str(input);
    // Bounded above by the depth check just made, so disabling serde's own
    // limit does not make unbounded recursion reachable.
    deserializer.disable_recursion_limit();

    let request = match Request::deserialize(&mut deserializer) {
        Ok(request) => request,
        Err(e) => return Response::err(format!("Request is not valid JSON: {e}")),
    };

    // The protocol is one request per run, so anything after the first JSON
    // value means the caller sent something other than what it intended.
    // Deserialize alone stops at the end of the first value and reports
    // success, so `{"action":"format","sql":"SELECT 1"}garbage` ran the format
    // and two concatenated requests ran the first and silently dropped the
    // second. end() is what makes a complete request distinguishable from a
    // truncated or accidentally concatenated one. It permits trailing
    // whitespace, which is why a newline terminated request still works.
    if let Err(e) = deserializer.end() {
        return Response::err(format!(
            "Request has trailing content after the first JSON value, and this protocol \
             carries one request per run: {e}"
        ));
    }

    dispatch(request)
}

/// Bounds the complete request's JSON nesting, including its outer object.
/// This is separate from the core's AST depth guards: dialects have different
/// limits, and an AST level can contain several JSON containers.
///
/// Keep input and parse output within the same bound. Runtimes impose their
/// own call stack budgets in addition to the module's 64 MiB linear stack.
/// Raising this requires release-WASI runtime tests on both amd64 and arm64
/// with multiple AST shapes, not just changing the constant.
const MAX_REQUEST_JSON_DEPTH: usize = 1536;

/// json_depth_exceeding returns the depth reached if it passes `limit`, and
/// None when the whole input stays within it.
///
/// Counting brackets outside string literals is enough here and needs no
/// allocation: a malformed request is rejected by the deserializer afterwards,
/// so this only has to be right about nesting, not about validity.
fn json_depth_exceeding(input: &str, limit: usize) -> Option<usize> {
    JsonDepth::default().exceeding(input.as_bytes(), limit)
}

/// Retains string/escape state across the serializer's individual writes.
#[derive(Default)]
struct JsonDepth {
    depth: usize,
    in_string: bool,
    escaped: bool,
}

impl JsonDepth {
    fn exceeding(&mut self, bytes: &[u8], limit: usize) -> Option<usize> {
        for &byte in bytes {
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                } else if byte == b'\\' {
                    self.escaped = true;
                } else if byte == b'"' {
                    self.in_string = false;
                }
                continue;
            }
            match byte {
                b'"' => self.in_string = true,
                b'{' | b'[' => {
                    self.depth += 1;
                    if self.depth > limit {
                        return Some(self.depth);
                    }
                }
                b'}' | b']' => self.depth = self.depth.saturating_sub(1),
                _ => {}
            }
        }
        None
    }
}

/// Checks the actual serialized shape without allocating JSON or visiting
/// beyond the supported depth. Reserve one level for the generate request's
/// outer object; the AST itself includes the statement array.
struct AstDepthWriter(JsonDepth);

impl AstDepthWriter {
    fn new() -> Self {
        Self(JsonDepth {
            depth: 1,
            ..Default::default()
        })
    }
}

impl Write for AstDepthWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.0.exceeding(bytes, MAX_REQUEST_JSON_DEPTH).is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Parsed AST exceeds the supported request JSON depth limit of \
                     {MAX_REQUEST_JSON_DEPTH}; reduce the query complexity"
                ),
            ));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
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
        Ok(ast) => {
            // ClickHouse permits deeper ASTs than the default parser guard.
            // Check the JSON shape before constructing a recursive Value, so
            // success always produces an AST that fits a generate request.
            if let Err(e) = serde_json::to_writer(AstDepthWriter::new(), &ast) {
                return Response::err(format!("Could not serialise the AST: {e}"));
            }
            match serde_json::to_value(&ast) {
                Ok(value) => Response::ok(value),
                Err(e) => Response::err(format!("Could not serialise the AST: {e}")),
            }
        }
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

/// Formats SQL with the library's formatter.
///
/// Not `transpile(sql, dialect, dialect)`, which was the first implementation:
/// that round trips through the generator in compact mode and applies
/// normalisation, so it returned single line SQL and dropped clauses the
/// formatter keeps, such as an explicit NULLS LAST. It also bypasses the
/// formatter's own guard path.
fn format(request: Request) -> Response {
    let dialect = match dialect_of(request.dialect.as_ref()) {
        Ok(d) => d,
        Err(e) => return Response::err(e),
    };
    let sql = match require_sql(&request) {
        Ok(sql) => sql,
        Err(e) => return Response::err(e),
    };

    match polyglot_sql::format(sql, dialect) {
        Ok(results) => Response::ok(serde_json::Value::from(results)),
        Err(e) => Response::err(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs one request through the real decoder, not through `dispatch`
    /// directly.
    ///
    /// The earlier helper built requests with `serde_json::from_value` and
    /// called `dispatch`, which skipped the request reader. That is why a
    /// round trip that failed over stdin passed in these tests: the reader is
    /// where serde_json's nesting limit lives. Everything below now goes
    /// through the same path a host does, bar the stdin read itself.
    fn call(body: serde_json::Value) -> Response {
        decode_and_dispatch(&body.to_string())
    }

    fn raw(input: &str) -> Response {
        decode_and_dispatch(input)
    }

    fn result_of(response: &Response) -> &serde_json::Value {
        response
            .result
            .as_ref()
            .unwrap_or_else(|| panic!("expected a result, got error {:?}", response.error))
    }

    /// An expression long enough to nest past serde_json's default limit of
    /// 128 JSON levels. `SELECT a + a + ... + a FROM t` parses into a
    /// left-leaning chain, so the term count sets the depth.
    fn long_expression(terms: usize) -> String {
        let mut sql = String::from("SELECT ");
        for i in 0..terms {
            if i > 0 {
                sql.push_str(" + ");
            }
            sql.push('a');
        }
        sql.push_str(" FROM t");
        sql
    }

    #[test]
    fn parse_then_generate_round_trips() {
        let sql = "SELECT a FROM t WHERE b = 1";
        let parsed = call(serde_json::json!({
            "action": "parse", "sql": sql, "dialect": "oracle"
        }));
        assert!(parsed.ok, "parse failed: {:?}", parsed.error);

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

    /// The protocol has to be able to carry whatever its own parser produced.
    /// At 61 terms the AST passes serde_json's default nesting limit of 128
    /// levels, and `generate` rejected an AST that `parse` had just returned
    /// with "recursion limit exceeded".
    /// Deep cases run on a thread with an explicit stack, the way the rest of
    /// the repository tests deep nesting. The WASI target links a 64 MiB stack
    /// for this, but a native test thread gets whatever the process gives it,
    /// which is not enough to deserialize an AST of this depth.
    fn with_large_stack(f: impl FnOnce() + Send + 'static) {
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(f)
            .expect("spawn")
            .join()
            .expect("the deep round trip must not overflow");
    }

    #[test]
    fn an_ast_the_parser_produced_can_be_sent_back() {
        with_large_stack(deep_round_trips);
    }

    fn deep_round_trips() {
        for terms in [40, 61, 120, 250, 500] {
            let sql = long_expression(terms);
            let parsed = call(serde_json::json!({
                "action": "parse", "sql": sql, "dialect": "oracle"
            }));
            assert!(parsed.ok, "{terms} terms: parse failed: {:?}", parsed.error);

            let generated = call(serde_json::json!({
                "action": "generate", "ast": result_of(&parsed), "dialect": "oracle"
            }));
            assert!(
                generated.ok,
                "{terms} terms: generate rejected an AST this module produced: {:?}",
                generated.error
            );
            let rendered = result_of(&generated).as_array().expect("array")[0]
                .as_str()
                .expect("string");
            assert_eq!(rendered, sql, "{terms} terms did not round trip");
        }
    }

    #[test]
    fn clickhouse_parse_output_fits_a_generate_request() {
        with_large_stack(|| {
            for terms in [500, 764, 765, 800, 1200, 2000, 4090] {
                let sql = long_expression(terms);
                let parsed = call(serde_json::json!({
                    "action": "parse", "sql": sql, "dialect": "clickhouse"
                }));
                if terms >= 765 {
                    assert!(!parsed.ok, "{terms} terms produced an unusable AST");
                    assert!(parsed.result.is_none());
                    assert!(
                        parsed
                            .error
                            .as_deref()
                            .unwrap_or_default()
                            .contains(&format!(
                                "request JSON depth limit of {MAX_REQUEST_JSON_DEPTH}"
                            )),
                        "{terms} terms: {:?}",
                        parsed.error
                    );
                    continue;
                }

                assert!(parsed.ok, "{terms} terms: {:?}", parsed.error);
                let request = serde_json::json!({
                    "action": "generate", "ast": result_of(&parsed), "dialect": "clickhouse"
                });
                assert_eq!(
                    json_depth_exceeding(&request.to_string(), MAX_REQUEST_JSON_DEPTH),
                    None
                );
                let generated = call(request);
                assert!(generated.ok, "{terms} terms: {:?}", generated.error);
                assert_eq!(result_of(&generated), &serde_json::json!([sql]));
            }
        });
    }

    #[test]
    fn parse_output_depth_ignores_escaped_string_contents() {
        let sql = format!("SELECT '{}'", r#"[]{}\""#.repeat(MAX_REQUEST_JSON_DEPTH));
        let parsed = call(serde_json::json!({
            "action": "parse", "sql": sql, "dialect": "oracle"
        }));
        assert!(
            parsed.ok,
            "literal contents counted as depth: {:?}",
            parsed.error
        );
        let generated = call(serde_json::json!({
            "action": "generate", "ast": result_of(&parsed), "dialect": "oracle"
        }));
        assert!(generated.ok, "generate failed: {:?}", generated.error);
        assert_eq!(result_of(&generated), &serde_json::json!([sql]));
    }

    /// Past the supported depth the answer has to be a reported error, not a
    /// stack overflow. In WASM an overflow reaches the host as an opaque trap
    /// with no message, so the bound is enforced before any AST is built.
    #[test]
    fn a_request_past_the_depth_limit_is_reported() {
        let deep = format!(
            "{{\"action\":\"generate\",\"ast\":{}}}",
            "[".repeat(MAX_REQUEST_JSON_DEPTH + 10) + &"]".repeat(MAX_REQUEST_JSON_DEPTH + 10)
        );
        let response = raw(&deep);
        assert!(!response.ok, "a request past the depth limit was accepted");
        let error = response.error.unwrap_or_default();
        assert!(
            error.contains("levels deep") && error.contains(&MAX_REQUEST_JSON_DEPTH.to_string()),
            "the error does not name the limit: {error}"
        );
    }

    #[test]
    fn json_depth_is_measured_outside_string_literals() {
        // Brackets inside a literal are data, not nesting.
        assert_eq!(json_depth_exceeding(r#"{"sql":"[[[[[["}"#, 2), None);
        // An escaped quote does not end the literal.
        assert_eq!(json_depth_exceeding(r#"{"sql":"a\"[[["}"#, 2), None);
        // Real nesting is counted.
        assert_eq!(json_depth_exceeding("[[[]]]", 2), Some(3));
        assert_eq!(json_depth_exceeding("[[]]", 2), None);
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

    /// Several statements in one request round trip as several results.
    #[test]
    fn multiple_statements_round_trip() {
        let parsed = call(serde_json::json!({
            "action": "parse", "sql": "SELECT 1 FROM DUAL; SELECT 2 FROM DUAL", "dialect": "oracle"
        }));
        assert!(parsed.ok, "parse failed: {:?}", parsed.error);
        assert_eq!(
            result_of(&parsed).as_array().map(Vec::len),
            Some(2),
            "expected two statements"
        );

        let generated = call(serde_json::json!({
            "action": "generate", "ast": result_of(&parsed), "dialect": "oracle"
        }));
        assert!(generated.ok, "generate failed: {:?}", generated.error);
        assert_eq!(
            result_of(&generated),
            &serde_json::json!(["SELECT 1 FROM DUAL", "SELECT 2 FROM DUAL"])
        );
    }

    #[test]
    fn transpile_works() {
        let transpiled = call(serde_json::json!({
            "action": "transpile", "sql": "SELECT a FROM t",
            "read_dialect": "oracle", "write_dialect": "duckdb"
        }));
        assert!(transpiled.ok, "transpile failed: {:?}", transpiled.error);
        assert_eq!(
            result_of(&transpiled),
            &serde_json::json!(["SELECT a FROM t"])
        );
    }

    /// `format` is the library's formatter, not a same-dialect transpile.
    ///
    /// The difference is visible two ways: the formatter pretty-prints over
    /// several lines, and it keeps clauses that a normalising round trip drops,
    /// such as an explicit NULLS LAST.
    #[test]
    fn format_pretty_prints_and_preserves_null_ordering() {
        let formatted = call(serde_json::json!({
            "action": "format",
            "sql": "SELECT a,b FROM t ORDER BY a NULLS LAST",
            "dialect": "postgres"
        }));
        assert!(formatted.ok, "format failed: {:?}", formatted.error);
        let rendered = result_of(&formatted).as_array().expect("array")[0]
            .as_str()
            .expect("string")
            .to_string();

        assert!(
            rendered.contains('\n'),
            "format returned single line output, so it is not using the formatter: {rendered:?}"
        );
        assert!(
            rendered.to_uppercase().contains("NULLS LAST"),
            "format dropped the explicit null ordering: {rendered:?}"
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

    /// Raw input that is not JSON at all, which the decoder sees before any
    /// field exists.
    #[test]
    fn malformed_raw_json_is_reported() {
        for input in [
            "",
            "not json",
            "{",
            "[1, 2, 3]",
            "{\"action\": }",
            "{\"action\": \"parse\", \"sql\": 7}",
        ] {
            let response = raw(input);
            assert!(!response.ok, "{input:?} was accepted");
            assert!(
                response.error.is_some(),
                "{input:?} failed with no reason given"
            );
        }
    }

    /// The protocol carries one request per run, so anything after the first
    /// JSON value means the caller sent something other than what it meant to.
    ///
    /// Deserialize alone stops at the end of the first value and reports
    /// success, so a request with trailing garbage ran and returned a result,
    /// and two concatenated requests ran the first and silently dropped the
    /// second. A caller had no way to tell a complete request from a truncated
    /// or accidentally joined one.
    #[test]
    fn trailing_content_after_a_request_is_refused() {
        let cases = [
            r#"{"action":"format","sql":"SELECT 1","dialect":"oracle"}garbage"#,
            // Two complete requests: the second would be discarded in silence.
            r#"{"action":"format","sql":"SELECT 1","dialect":"oracle"}{"action":"format","sql":"SELECT 2","dialect":"oracle"}"#,
            r#"{"action":"format","sql":"SELECT 1","dialect":"oracle"}{}"#,
            r#"{"action":"format","sql":"SELECT 1","dialect":"oracle"}]"#,
            r#"{"action":"format","sql":"SELECT 1","dialect":"oracle"} null"#,
        ];
        for input in cases {
            let response = raw(input);
            assert!(!response.ok, "{input:?} was accepted");
            let error = response.error.unwrap_or_default();
            assert!(
                error.contains("trailing content"),
                "{input:?} was refused for the wrong reason: {error}"
            );
        }
    }

    /// Whitespace around a request is not trailing content. A host writing a
    /// newline terminated request is doing nothing wrong.
    #[test]
    fn whitespace_around_a_request_is_allowed() {
        for input in [
            "{\"action\":\"format\",\"sql\":\"SELECT 1\",\"dialect\":\"oracle\"}\n",
            "  {\"action\":\"format\",\"sql\":\"SELECT 1\",\"dialect\":\"oracle\"}  ",
            "\n\t{\"action\":\"format\",\"sql\":\"SELECT 1\",\"dialect\":\"oracle\"}\r\n",
        ] {
            let response = raw(input);
            assert!(response.ok, "{input:?} was refused: {:?}", response.error);
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
