use std::path::{Path, PathBuf};

use url::Url;

use crate::origin::{OriginError, accept_webview_url, query_value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenerAction {
    ShowWindow,
    PickFolder,
    Register { path: PathBuf },
}

pub const PROTOCOL_SCHEMES: &[&str] = &["bildr", "harness"];

pub fn parse_opener(argument: &str) -> Result<OpenerAction, OriginError> {
    let value = argument.trim();
    if value.is_empty() {
        return Ok(OpenerAction::ShowWindow);
    }
    let url = Url::parse(value).map_err(|error| OriginError(format!("invalid opener: {error}")))?;
    if PROTOCOL_SCHEMES.contains(&url.scheme()) {
        return parse_app_scheme(&url);
    }
    if url.scheme() == "http" {
        let accepted = accept_webview_url(value)?;
        if let Some(path) = query_value(&accepted, "register")
            && !path.is_empty()
        {
            return Ok(OpenerAction::Register {
                path: PathBuf::from(path),
            });
        }
        return Ok(OpenerAction::ShowWindow);
    }
    Err(OriginError(format!(
        "unsupported opener scheme {}",
        url.scheme()
    )))
}

fn parse_app_scheme(url: &Url) -> Result<OpenerAction, OriginError> {
    let host = url.host_str().unwrap_or("").trim();
    let path = url.path().trim_matches('/');
    let command = if !host.is_empty() { host } else { path };
    let command = if command.is_empty() { "open" } else { command };
    match command {
        "open" | "show" => Ok(OpenerAction::ShowWindow),
        "pick-folder" | "pick_folder" | "browse" => Ok(OpenerAction::PickFolder),
        "register" => {
            let path = query_value(url, "path").or_else(|| query_value(url, "register"));
            let Some(path) = path.filter(|value| !value.is_empty()) else {
                return Ok(OpenerAction::PickFolder);
            };
            Ok(OpenerAction::Register {
                path: PathBuf::from(path),
            })
        }
        other => Err(OriginError(format!("unknown opener command {other}"))),
    }
}

pub fn register_query_url(origin: &Url, folder: &Path) -> Result<Url, OriginError> {
    let origin = accept_webview_url(origin.as_str())?;
    let path = folder
        .to_str()
        .ok_or_else(|| OriginError("repository folder path is not valid UTF-8".to_owned()))?;
    Ok(crate::origin::with_query(&origin, "register", path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_opener_shows_the_window() {
        assert_eq!(
            parse_opener("bildr://open").expect("open"),
            OpenerAction::ShowWindow
        );
        assert_eq!(
            parse_opener("harness://show").expect("show"),
            OpenerAction::ShowWindow
        );
        assert_eq!(
            parse_opener("http://127.0.0.1:7310/?shell=desktop").expect("http"),
            OpenerAction::ShowWindow
        );
    }

    #[test]
    fn protocol_opener_picks_a_folder_or_registers() {
        assert_eq!(
            parse_opener("bildr://pick-folder").expect("pick"),
            OpenerAction::PickFolder
        );
        assert_eq!(
            parse_opener("bildr://register?path=/home/src/app").expect("register"),
            OpenerAction::Register {
                path: PathBuf::from("/home/src/app")
            }
        );
        let origin = accept_webview_url("http://127.0.0.1:7310/?shell=desktop").expect("origin");
        let url = register_query_url(&origin, Path::new("/home/src/app")).expect("query");
        assert_eq!(
            query_value(&url, "register").as_deref(),
            Some("/home/src/app")
        );
        assert_eq!(query_value(&url, "shell").as_deref(), Some("desktop"));
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
    }

    #[test]
    fn non_loopback_http_opener_is_rejected() {
        assert!(parse_opener("http://example.com/").is_err());
        assert!(parse_opener("https://127.0.0.1:7310/").is_err());
    }
}
