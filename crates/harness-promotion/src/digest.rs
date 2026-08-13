use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ContractError {
    #[error("invalid contract")]
    Invalid,
    #[error("digest mismatch")]
    Digest,
    #[error("hard gate failed")]
    HardGate,
    #[error("stage ordering violation")]
    StageOrder,
    #[error("exposure stopped")]
    Stopped,
    #[error("exposure budget exhausted")]
    Budget,
    #[error("stale binding")]
    Stale,
    #[error("missing approval or receipt")]
    Missing,
}
pub fn digest_without_self<T: Serialize>(value: &T) -> Result<String, ContractError> {
    let mut value = serde_json::to_value(value).map_err(|_| ContractError::Invalid)?;
    value
        .as_object_mut()
        .ok_or(ContractError::Invalid)?
        .remove("sha256");
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(&canonical(value)).map_err(|_| ContractError::Invalid)?,
    )))
}
pub fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b.is_ascii_lowercase() && b.is_ascii_hexdigit()))
}
pub fn id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}
fn canonical(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted = map
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(sorted.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        value => value,
    }
}
