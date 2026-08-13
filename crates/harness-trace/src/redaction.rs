use serde_json::{Value, json};

pub(crate) fn redact(payload: &Value, input_class: &str) -> (Value, String) {
    let reasoning = payload
        .pointer("/item/type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind == "reasoning")
        || input_class.contains("reasoning");
    let class = if reasoning {
        "private_reasoning_removed"
    } else if input_class.contains("secret") {
        "secret_removed"
    } else if input_class.contains("customer") {
        "customer_data_removed"
    } else if input_class == "none" {
        "none"
    } else {
        "content_withheld"
    };
    let output_class = if class == "none" {
        "content_withheld"
    } else {
        class
    };
    (
        json!({"redacted": output_class, "method_shape": shape(payload)}),
        output_class.to_owned(),
    )
}

fn shape(payload: &Value) -> Value {
    match payload {
        // Field names themselves can contain customer or credential material.
        // Preserve only a bounded structural shape, never ingress keys or values.
        Value::Object(map) => json!({"object_fields": map.len()}),
        Value::Array(values) => json!({"array_items": values.len()}),
        Value::String(_) => Value::String("[redacted]".to_owned()),
        Value::Number(_) => Value::String("[number]".to_owned()),
        Value::Bool(_) => Value::String("[boolean]".to_owned()),
        Value::Null => Value::Null,
    }
}
