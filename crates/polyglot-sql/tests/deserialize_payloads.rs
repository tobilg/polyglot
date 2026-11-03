//! Exercise the WASI deserialization attribute on the host as well, so its
//! forwarding behavior and wire format do not depend on runtime-only tests.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};

static PAYLOAD_READS: AtomicUsize = AtomicUsize::new(0);

#[inline(never)]
fn read_payload<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    PAYLOAD_READS.fetch_add(1, Ordering::Relaxed);
    T::deserialize(deserializer)
}

#[polyglot_sql_ast_derive::deserialize_payloads_with("read_payload")]
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Expression {
    Literal(String),
    #[serde(rename = "sum")]
    Add(Box<Binary>),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Binary {
    left: Expression,
    right: Expression,
}

#[test]
fn payload_helpers_preserve_serde_round_trips_and_errors() {
    let json = serde_json::json!({"sum": {
        "left": {"literal": "1"},
        "right": {"literal": "2"}
    }});
    let expression: Expression = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(PAYLOAD_READS.load(Ordering::Relaxed), 3);
    assert_eq!(serde_json::to_value(&expression).unwrap(), json);

    let text = serde_json::to_string(&expression).unwrap();
    assert_eq!(
        serde_json::from_str::<Expression>(&text).unwrap(),
        expression
    );
    assert_eq!(PAYLOAD_READS.load(Ordering::Relaxed), 6);

    let error =
        serde_json::from_value::<Expression>(serde_json::json!({"unknown": null})).unwrap_err();
    assert!(error.to_string().contains("unknown variant"));
    let error = serde_json::from_value::<Expression>(serde_json::json!({
        "sum": {"left": {"literal": "1"}}
    }))
    .unwrap_err();
    assert!(error.to_string().contains("missing field `right`"));
}
