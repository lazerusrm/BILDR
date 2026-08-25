use serde::Deserialize;

pub const CAPABILITY_JSON: &str = include_str!("../capabilities/default.json");
pub const TAURI_CONF_JSON: &str = include_str!("../tauri.conf.json");
pub const CARGO_MANIFEST: &str = include_str!("../Cargo.toml");

pub const IPC_COMMANDS: &[&str] = &["show_window", "pick_repository_folder"];

pub const WEBVIEW_ENGINE: &str = "wry-os-webview";

const FORBIDDEN_COMMAND_TOKENS: &[&str] = &[
    "create_run",
    "create-run",
    "approve_plan",
    "approve-plan",
    "approve_signoff",
    "git_commit",
    "git_push",
    "git_merge",
    "codex_exec",
    "sqlite",
    "orchestrat",
    "worktree_create",
];

const FORBIDDEN_PERMISSION_PREFIXES: &[&str] = &[
    "fs:",
    "shell:",
    "http:",
    "sql:",
    "core:shell",
    "os:allow-execute",
];

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CapabilityDocument {
    pub identifier: String,
    pub windows: Vec<String>,
    pub permissions: Vec<String>,
}

pub fn shipped_capabilities() -> CapabilityDocument {
    serde_json::from_str(CAPABILITY_JSON).expect("capabilities/default.json must parse")
}

pub fn permission_is_shell_class(permission: &str) -> bool {
    if FORBIDDEN_PERMISSION_PREFIXES
        .iter()
        .any(|prefix| permission.starts_with(prefix))
    {
        return false;
    }
    if command_is_harness_mutation(permission) {
        return false;
    }
    permission == "core:default"
        || permission.starts_with("core:window:")
        || permission.starts_with("core:app:")
        || permission.starts_with("core:event:")
        || permission.starts_with("core:menu:")
        || permission.starts_with("core:tray:")
        || permission.starts_with("core:webview:")
        || permission.starts_with("core:path:")
        || permission.starts_with("core:resources:")
        || permission.starts_with("core:image:")
        || permission.starts_with("dialog:")
        || permission.starts_with("notification:")
        || permission.starts_with("deep-link:")
}

pub fn command_is_harness_mutation(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    FORBIDDEN_COMMAND_TOKENS
        .iter()
        .any(|token| lowered == *token || lowered.contains(token))
}

pub fn allowlist_exposes_harness_mutation(doc: &CapabilityDocument) -> bool {
    doc.permissions
        .iter()
        .any(|permission| !permission_is_shell_class(permission))
        || IPC_COMMANDS
            .iter()
            .copied()
            .any(command_is_harness_mutation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_allowlist_is_shell_class_only() {
        let doc = shipped_capabilities();
        assert_eq!(doc.identifier, "default");
        assert!(doc.windows.iter().any(|window| window == "main"));
        assert!(!doc.permissions.is_empty());
        for permission in &doc.permissions {
            assert!(
                permission_is_shell_class(permission),
                "non-shell permission {permission}"
            );
        }
        assert!(!allowlist_exposes_harness_mutation(&doc));
        for command in IPC_COMMANDS {
            assert!(
                !command_is_harness_mutation(command),
                "IPC command {command} mutates the harness"
            );
        }
        for token in [
            "create_run",
            "approve_plan",
            "git_commit",
            "codex_exec",
            "sqlite",
        ] {
            assert!(
                !CAPABILITY_JSON.contains(token),
                "capability JSON mentions harness mutation {token}"
            );
            assert!(
                !IPC_COMMANDS.contains(&token),
                "IPC allowlist includes {token}"
            );
        }
    }

    #[test]
    fn manifest_uses_tauri_wry_and_not_a_second_controller() {
        assert!(CARGO_MANIFEST.contains("tauri"));
        assert!(CARGO_MANIFEST.contains("tray-icon"));
        assert!(!CARGO_MANIFEST.contains("electron"));
        assert!(!CARGO_MANIFEST.contains("cef"));
        assert!(!CARGO_MANIFEST.contains("webengine"));
        assert!(!CARGO_MANIFEST.contains("harness-orchestrator"));
        assert!(!CARGO_MANIFEST.contains("harness-git"));
        assert!(!CARGO_MANIFEST.contains("harness-codex"));
        assert!(!CARGO_MANIFEST.contains("harness-store"));
        let conf: serde_json::Value =
            serde_json::from_str(TAURI_CONF_JSON).expect("tauri.conf.json");
        assert_eq!(conf["identifier"], "app.bildr.desktop");
        assert_eq!(conf["app"]["withGlobalTauri"], false);
        assert_eq!(WEBVIEW_ENGINE, "wry-os-webview");
    }
}
