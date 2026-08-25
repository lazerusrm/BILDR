use std::path::PathBuf;

use clap::Parser;

use crate::origin::DEFAULT_ORIGIN;

#[derive(Debug, Clone, Parser, PartialEq, Eq)]
#[command(
    name = "harness-desktop",
    version,
    about = "Native OS-webview shell for the BILDR operator console"
)]
pub struct DesktopOptions {
    /// Loopback HTTP origin served by harnessd.
    #[arg(long, env = "HARNESS_URL", default_value = DEFAULT_ORIGIN)]
    pub url: String,
    /// Path to the harnessd binary. Defaults to a sibling of this executable.
    #[arg(long, env = "HARNESSD")]
    pub harnessd: Option<PathBuf>,
    /// Start or attach to harnessd without Codex (inspection-only).
    #[arg(long, env = "HARNESS_DESKTOP_WITHOUT_CODEX")]
    pub without_codex: bool,
    /// Kill a harnessd process this shell started when the window exits.
    #[arg(long)]
    pub own_sidecar: bool,
    /// Deep link or protocol URL (bildr://open, bildr://pick-folder).
    #[arg(value_name = "OPENER")]
    pub opener: Option<String>,
}

impl DesktopOptions {
    pub fn from_args() -> Self {
        Self::parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn defaults_to_loopback_http() {
        let options = DesktopOptions::try_parse_from(["harness-desktop"]).expect("parse");
        assert_eq!(options.url, DEFAULT_ORIGIN);
        assert!(!options.without_codex);
        assert!(!options.own_sidecar);
        assert_eq!(options.opener, None);
    }

    #[test]
    fn accepts_inspection_sidecar_and_protocol_opener() {
        let options = DesktopOptions::try_parse_from([
            "harness-desktop",
            "--without-codex",
            "--own-sidecar",
            "--url",
            "http://127.0.0.1:7310",
            "bildr://open",
        ])
        .expect("parse");
        assert!(options.without_codex);
        assert!(options.own_sidecar);
        assert_eq!(options.opener.as_deref(), Some("bildr://open"));
        assert_eq!(options.url, "http://127.0.0.1:7310");
    }
}
