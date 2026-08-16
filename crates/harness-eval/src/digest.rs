use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum Split {
    Training,
    Development,
    Holdout,
    Canary,
    Quarantine,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Principal {
    Optimizer,
    CandidateRuntime,
    Evaluator,
    Operator,
}
#[derive(Debug, Error, Eq, PartialEq)]
pub enum DigestError {
    #[error("invalid immutable record: {0}")]
    Invalid(&'static str),
    #[error("self digest mismatch")]
    DigestMismatch,
}
pub fn canonical_digest_without_self<T: Serialize>(value: &T) -> Result<String, DigestError> {
    let mut v = serde_json::to_value(value).map_err(|_| DigestError::Invalid("serialization"))?;
    v.as_object_mut()
        .ok_or(DigestError::Invalid("object"))?
        .remove("sha256");
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(&canonical(v)).map_err(|_| DigestError::Invalid("canonical"))?,
    )))
}
pub fn hash(v: &str) -> bool {
    v.len() == 64
        && v.bytes()
            .all(|b| b.is_ascii_digit() || (b.is_ascii_lowercase() && b.is_ascii_hexdigit()))
}
pub fn token(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 128
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}
pub fn controller_identifier(v: &str) -> bool {
    !v.is_empty()
        && v.len() <= 160
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}
pub fn sha40(v: &str) -> bool {
    v.len() == 40
        && v.bytes()
            .all(|b| b.is_ascii_digit() || (b.is_ascii_lowercase() && b.is_ascii_hexdigit()))
}
fn canonical(v: Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut x = m.into_iter().collect::<Vec<_>>();
            x.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(x.into_iter().map(|(k, v)| (k, canonical(v))).collect())
        }
        Value::Array(x) => Value::Array(x.into_iter().map(canonical).collect()),
        x => x,
    }
}
