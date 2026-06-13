//! Compatibility helpers for Expression JSON accepted at public API boundaries.
//!
//! The canonical AST representation is the serde JSON form of [`Expression`].
//! Some wrappers historically exposed JavaScript values that stringify unit
//! variants such as `Expression::Null` as `{}`. These helpers repair that
//! narrow compatibility shape while preserving errors for unknown AST forms.

use crate::expressions::Expression;
use serde_json::{json, Value};

/// Deserialize a single [`Expression`] from JSON, accepting known compatibility
/// shapes used by SDK boundaries.
pub fn expression_from_str(input: &str) -> Result<Expression, String> {
    let value: Value = serde_json::from_str(input).map_err(|error| error.to_string())?;
    expression_from_value(value)
}

/// Deserialize a list of [`Expression`] values from JSON, accepting known
/// compatibility shapes used by SDK boundaries.
pub fn expressions_from_str(input: &str) -> Result<Vec<Expression>, String> {
    let value: Value = serde_json::from_str(input).map_err(|error| error.to_string())?;
    expressions_from_value(value)
}

/// Deserialize a single [`Expression`] from a JSON value.
pub fn expression_from_value(value: Value) -> Result<Expression, String> {
    match serde_json::from_value::<Expression>(value.clone()) {
        Ok(expression) => Ok(expression),
        Err(original_error) => {
            let mut repaired = value;
            normalize_expression_value(&mut repaired);
            serde_json::from_value::<Expression>(repaired)
                .map_err(|error| format!("{error}; original error: {original_error}"))
        }
    }
}

/// Deserialize a list of [`Expression`] values from a JSON value.
pub fn expressions_from_value(value: Value) -> Result<Vec<Expression>, String> {
    match serde_json::from_value::<Vec<Expression>>(value.clone()) {
        Ok(expressions) => Ok(expressions),
        Err(original_error) => {
            let mut repaired = value;
            if let Value::Array(items) = &mut repaired {
                for item in items {
                    normalize_expression_value(item);
                }
            }
            serde_json::from_value::<Vec<Expression>>(repaired)
                .map_err(|error| format!("{error}; original error: {original_error}"))
        }
    }
}

/// Serialize an [`Expression`] through serde JSON first so wrapper layers expose
/// canonical JSON-compatible values instead of runtime-specific JS shapes.
pub fn expression_to_value(expression: &Expression) -> Result<Value, String> {
    serde_json::to_value(expression).map_err(|error| error.to_string())
}

fn normalize_expression_value(value: &mut Value) {
    match value {
        Value::Object(map) if map.is_empty() => {
            *value = json!({ "null": null });
        }
        Value::Object(map) if map.len() == 1 => {
            let (kind, payload) = map.iter_mut().next().expect("single entry exists");
            if kind == "null" && matches!(payload, Value::Object(inner) if inner.is_empty()) {
                *payload = Value::Null;
                return;
            }

            if kind == "is_null" {
                normalize_is_null_payload(payload);
            }

            match payload {
                Value::Object(payload_map) => normalize_struct_fields(payload_map),
                Value::Array(items) => {
                    for item in items {
                        normalize_expression_value(item);
                    }
                }
                _ => {}
            }
        }
        Value::Object(map) => normalize_struct_fields(map),
        Value::Array(items) => {
            for item in items {
                normalize_expression_value(item);
            }
        }
        _ => {}
    }
}

fn normalize_is_null_payload(payload: &mut Value) {
    let Value::Array(items) = payload else {
        return;
    };
    if items.len() != 1 {
        return;
    }

    let mut this = items.remove(0);
    normalize_expression_value(&mut this);
    *payload = json!({
        "this": this,
        "not": false,
        "postfix_form": false
    });
}

fn normalize_struct_fields(map: &mut serde_json::Map<String, Value>) {
    for (key, value) in map.iter_mut() {
        if is_expression_array_field(key) {
            if let Value::Array(items) = value {
                for item in items {
                    normalize_expression_value(item);
                }
                continue;
            }
        }

        if is_expression_field(key) {
            normalize_expression_value(value);
        } else {
            normalize_container_value(value);
        }
    }
}

fn normalize_container_value(value: &mut Value) {
    match value {
        Value::Object(map) => normalize_struct_fields(map),
        Value::Array(items) => {
            for item in items {
                normalize_container_value(item);
            }
        }
        _ => {}
    }
}

fn is_expression_field(key: &str) -> bool {
    matches!(
        key,
        "this"
            | "left"
            | "right"
            | "low"
            | "high"
            | "expression"
            | "condition"
            | "query"
            | "unnest"
            | "on"
            | "prewhere"
            | "where"
            | "where_clause"
            | "having"
            | "qualify"
            | "source"
            | "body"
            | "default"
            | "true_value"
            | "false_value"
            | "else_result"
            | "target"
            | "format"
            | "limit"
            | "offset"
    )
}

fn is_expression_array_field(key: &str) -> bool {
    matches!(
        key,
        "expressions"
            | "args"
            | "columns"
            | "values"
            | "partition_by"
            | "order_by"
            | "group_by"
            | "distinct_on"
            | "limit_by"
            | "settings"
            | "for_xml"
            | "for_json"
            | "exclude"
            | "hints"
            | "on_columns"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::Expression;

    fn column_json(name: &str) -> Value {
        json!({
            "column": {
                "name": {
                    "name": name,
                    "quoted": false,
                    "trailing_comments": [],
                    "span": null
                },
                "table": null,
                "join_mark": false,
                "trailing_comments": [],
                "span": null,
                "inferred_type": null
            }
        })
    }

    #[test]
    fn expression_from_value_accepts_legacy_empty_object_as_null() {
        assert!(matches!(
            expression_from_value(json!({})).expect("empty object should become NULL"),
            Expression::Null(_)
        ));
    }

    #[test]
    fn expression_from_value_accepts_empty_null_payload() {
        assert!(matches!(
            expression_from_value(json!({ "null": {} })).expect("empty null payload should work"),
            Expression::Null(_)
        ));
    }

    #[test]
    fn expression_from_value_repairs_nested_null_expression_fields() {
        let expression = expression_from_value(json!({
            "between": {
                "this": column_json("created_at"),
                "low": { "literal": { "literal_type": "string", "value": "2024-01-01" } },
                "high": {},
                "not": false,
                "symmetric": null
            }
        }))
        .expect("between high bound should be repaired");

        match expression {
            Expression::Between(between) => assert!(matches!(between.high, Expression::Null(_))),
            other => panic!("expected between expression, got {other:?}"),
        }
    }

    #[test]
    fn expression_from_value_accepts_is_null_array_shorthand() {
        let expression = expression_from_value(json!({
            "is_null": [column_json("deleted_at")]
        }))
        .expect("is_null shorthand should be repaired");

        match expression {
            Expression::IsNull(is_null) => {
                assert!(!is_null.not);
                assert!(!is_null.postfix_form);
                assert!(matches!(is_null.this, Expression::Column(_)));
            }
            other => panic!("expected is_null expression, got {other:?}"),
        }
    }

    #[test]
    fn expression_from_value_keeps_unknown_ast_as_error() {
        let error = expression_from_value(json!({ "not_a_real_expression": {} }))
            .expect_err("unknown expression should stay invalid");
        assert!(error.contains("not_a_real_expression"));
    }
}
