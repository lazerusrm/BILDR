use std::path::{Path, PathBuf};

use url::Url;

use crate::origin::{OriginError, accept_webview_url, query_value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenerAction {
    ShowWindow,
    PickFolder { new_project: bool },
    Register { path: PathBuf },
    NewProject { parent_path: PathBuf },
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
        if let Some(parent_path) = query_value(&accepted, "new_project_parent")
            && !parent_path.is_empty()
        {
            return Ok(OpenerAction::NewProject {
                parent_path: PathBuf::from(parent_path),
            });
        }
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
        "pick-folder" | "pick_folder" | "browse" => Ok(OpenerAction::PickFolder {
            new_project: match query_value(url, "purpose").as_deref() {
                None | Some("repository") => false,
                Some("new-project") => true,
                Some(other) => {
                    return Err(OriginError(format!(
                        "unknown folder-picker purpose {other}"
                    )));
                }
            },
        }),
        "register" => {
            let path = query_value(url, "path").or_else(|| query_value(url, "register"));
            let Some(path) = path.filter(|value| !value.is_empty()) else {
                return Ok(OpenerAction::PickFolder { new_project: false });
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

pub fn new_project_query_url(origin: &Url, folder: &Path) -> Result<Url, OriginError> {
    let origin = accept_webview_url(origin.as_str())?;
    let path = folder
        .to_str()
        .ok_or_else(|| OriginError("new project folder path is not valid UTF-8".to_owned()))?;
    Ok(crate::origin::with_query(
        &origin,
        "new_project_parent",
        path,
    ))
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
            OpenerAction::PickFolder { new_project: false }
        );
        assert_eq!(
            parse_opener("bildr://pick-folder?purpose=new-project").expect("new project pick"),
            OpenerAction::PickFolder { new_project: true }
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
        let project_url = new_project_query_url(&origin, Path::new("/home/src")).expect("project");
        assert_eq!(
            query_value(&project_url, "new_project_parent").as_deref(),
            Some("/home/src")
        );
    }

    #[test]
    fn non_loopback_http_opener_is_rejected() {
        assert!(parse_opener("http://example.com/").is_err());
        assert!(parse_opener("https://127.0.0.1:7310/").is_err());
    }
}
