use std::net::IpAddr;

use url::Url;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct OriginError(pub String);

pub const DEFAULT_ORIGIN: &str = "http://127.0.0.1:7310";
pub const DESKTOP_SHELL_QUERY: &str = "desktop";

pub fn accept_webview_url(value: &str) -> Result<Url, OriginError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(OriginError("webview URL is empty".to_owned()));
    }
    let url = Url::parse(trimmed)
        .map_err(|error| OriginError(format!("invalid webview URL {trimmed}: {error}")))?;
    if url.scheme() != "http" {
        return Err(OriginError(format!(
            "webview URL must be loopback HTTP, got {}",
            url.scheme()
        )));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(OriginError(
            "webview URL must not include credentials".to_owned(),
        ));
    }
    if !is_loopback_host(&url) {
        return Err(OriginError(format!(
            "webview URL host is not loopback: {trimmed}"
        )));
    }
    Ok(url)
}

pub fn is_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

pub fn origin_header(url: &Url) -> String {
    let host = match url.host() {
        Some(url::Host::Ipv6(ip)) => format!("[{ip}]"),
        Some(url::Host::Domain(domain)) => domain.to_owned(),
        Some(url::Host::Ipv4(ip)) => ip.to_string(),
        None => "127.0.0.1".to_owned(),
    };
    match url.port() {
        Some(port) => format!("{}://{host}:{port}", url.scheme()),
        None => format!("{}://{host}", url.scheme()),
    }
}

pub fn host_header(url: &Url) -> String {
    let host = match url.host() {
        Some(url::Host::Ipv6(ip)) => format!("[{ip}]"),
        Some(url::Host::Domain(domain)) => domain.to_owned(),
        Some(url::Host::Ipv4(ip)) => ip.to_string(),
        None => "127.0.0.1".to_owned(),
    };
    match url.port_or_known_default() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    }
}

pub fn bind_address(url: &Url) -> Result<String, OriginError> {
    let _ = accept_webview_url(url.as_str())?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| OriginError("webview URL is missing a port".to_owned()))?;
    let host = match url.host() {
        Some(url::Host::Ipv6(ip)) => format!("[{ip}]"),
        Some(url::Host::Ipv4(ip)) => ip.to_string(),
        Some(url::Host::Domain(_)) => "127.0.0.1".to_owned(),
        None => "127.0.0.1".to_owned(),
    };
    Ok(format!("{host}:{port}"))
}

pub fn desktop_shell_url(url: &Url) -> Url {
    with_query(url, "shell", DESKTOP_SHELL_QUERY)
}

pub fn with_query(url: &Url, key: &str, value: &str) -> Url {
    let mut url = url.clone();
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(name, item)| (name.into_owned(), item.into_owned()))
        .filter(|(name, _)| name != key)
        .collect();
    pairs.push((key.to_owned(), value.to_owned()));
    url.set_query(None);
    {
        let mut serializer = url.query_pairs_mut();
        for (name, item) in &pairs {
            serializer.append_pair(name, item);
        }
    }
    url
}

pub fn query_value(url: &Url, key: &str) -> Option<String> {
    url.query_pairs()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
}

pub fn loopback_ip(url: &Url) -> Option<IpAddr> {
    match url.host() {
        Some(url::Host::Ipv4(ip)) => Some(IpAddr::V4(ip)),
        Some(url::Host::Ipv6(ip)) => Some(IpAddr::V6(ip)),
        Some(url::Host::Domain(domain)) if domain.eq_ignore_ascii_case("localhost") => {
            Some(IpAddr::V4(std::net::Ipv4Addr::LOCALHOST))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_http_origins_are_accepted() {
        for value in [
            "http://127.0.0.1:7310",
            "http://localhost:7310",
            "http://[::1]:7310",
            "http://127.0.0.1:7310/?shell=desktop",
            " http://127.1.2.3:9 ",
        ] {
            accept_webview_url(value).unwrap_or_else(|error| panic!("{value}: {error}"));
        }
    }

    #[test]
    fn non_loopback_and_non_http_origins_are_rejected() {
        for value in [
            "",
            "http://192.168.1.10:7310",
            "http://10.0.0.2:7310",
            "http://0.0.0.0:7310",
            "http://example.com:7310",
            "https://127.0.0.1:7310",
            "file:///tmp/index.html",
            "tauri://localhost",
            "http://user@127.0.0.1:7310",
            "http://127.0.0.1.example:7310",
        ] {
            assert!(
                accept_webview_url(value).is_err(),
                "expected rejection for {value}"
            );
        }
    }

    #[test]
    fn desktop_shell_query_is_idempotent() {
        let url = accept_webview_url(DEFAULT_ORIGIN).expect("default origin");
        let once = desktop_shell_url(&url);
        let twice = desktop_shell_url(&once);
        assert_eq!(query_value(&once, "shell").as_deref(), Some("desktop"));
        assert_eq!(once.as_str(), twice.as_str());
        assert_eq!(once.host_str(), Some("127.0.0.1"));
        assert_eq!(once.scheme(), "http");
    }
}
