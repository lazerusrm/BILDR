use crate::{Principal, Split};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldoutState {
    Clean,
    Invalidated,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HoldoutAccess {
    pub principal: Principal,
    pub split: Split,
    pub action: HoldoutAction,
    pub receipt_id: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LeakageDeclaration {
    pub case_id: String,
    pub rotation_revision: u64,
    pub confirmed: bool,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HoldoutAction {
    ReadMetadata,
    ReadAnswer,
    Execute,
}
#[derive(Debug, Error, Eq, PartialEq)]
pub enum CustodyError {
    #[error("holdout access denied")]
    Denied,
    #[error("holdout leakage invalidates experiment")]
    Invalidated,
}
pub fn authorize(access: &HoldoutAccess) -> Result<(), CustodyError> {
    if access.split != Split::Holdout {
        return Ok(());
    }
    matches!(access.principal, Principal::Evaluator | Principal::Operator)
        .then_some(())
        .ok_or(CustodyError::Denied)
}
pub fn leakage_state(declarations: &[LeakageDeclaration]) -> HoldoutState {
    if declarations.iter().any(|a| a.confirmed) {
        HoldoutState::Invalidated
    } else {
        HoldoutState::Clean
    }
}
pub fn require_clean_holdout(declarations: &[LeakageDeclaration]) -> Result<(), CustodyError> {
    (leakage_state(declarations) == HoldoutState::Clean)
        .then_some(())
        .ok_or(CustodyError::Invalidated)
}
