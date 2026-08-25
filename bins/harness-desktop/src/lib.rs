//! Tauri 2 / wry OS-webview shell for BILDR.
//!
//! The window loads the existing harnessd loopback UI. This crate does not
//! perform Git, Codex, orchestration, or SQLite mutations.

mod app;
pub mod capabilities;
pub mod notify;
pub mod opener;
pub mod options;
pub mod origin;
pub mod sidecar;
pub mod webview_env;

pub use app::run;
pub use capabilities::{IPC_COMMANDS, WEBVIEW_ENGINE, shipped_capabilities};
pub use opener::{OpenerAction, new_project_query_url, parse_opener, register_query_url};
pub use origin::{accept_webview_url, desktop_shell_url};
pub use sidecar::{DaemonPlan, execute_daemon_plan, plan_daemon_lifecycle};
