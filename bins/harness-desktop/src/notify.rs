use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use url::Url;

use crate::origin::{OriginError, accept_webview_url, host_header, origin_header};
use crate::sidecar::socket_addr_for_url;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct NotifyError(pub String);

impl From<OriginError> for NotifyError {
    fn from(error: OriginError) -> Self {
        Self(error.0)
    }
}

pub fn pending_approval_notification(previous: usize, current: usize) -> Option<String> {
    if current == 0 || current <= previous {
        return None;
    }
    Some(if current == 1 {
        "1 pending approval".to_owned()
    } else {
        format!("{current} pending approvals")
    })
}

pub fn pending_approval_count_from_json(body: &str) -> Result<usize, NotifyError> {
    let value: serde_json::Value = serde_json::from_str(body)
        .map_err(|error| NotifyError(format!("approvals JSON: {error}")))?;
    match value {
        serde_json::Value::Array(items) => Ok(items.len()),
        _ => Err(NotifyError(
            "approvals endpoint did not return an array".to_owned(),
        )),
    }
}

pub fn fetch_pending_approval_count(origin: &Url) -> Result<usize, NotifyError> {
    let origin = accept_webview_url(origin.as_str())?;
    let session = create_local_session(&origin)?;
    let mut url = origin.clone();
    url.set_path("/api/v1/approvals");
    url.set_query(Some("state=pending"));
    let cookie = format!("harness_session={session}");
    let origin_value = origin_header(&origin);
    let response = http_request(
        "GET",
        &url,
        &[
            ("Cookie", cookie.as_str()),
            ("Origin", origin_value.as_str()),
        ],
        b"",
    )?;
    if !http_success(response.status) {
        return Err(NotifyError(format!("approvals HTTP {}", response.status)));
    }
    pending_approval_count_from_json(&response.body)
}

fn http_success(status: u16) -> bool {
    (200..300).contains(&status)
}

struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

fn create_local_session(origin: &Url) -> Result<String, NotifyError> {
    let mut url = origin.clone();
    url.set_path("/api/v1/session");
    url.set_query(None);
    let origin_value = origin_header(origin);
    let response = http_request("POST", &url, &[("Origin", origin_value.as_str())], b"")?;
    if !http_success(response.status) {
        return Err(NotifyError(format!("session HTTP {}", response.status)));
    }
    cookie_value(&response, "harness_session")
        .ok_or_else(|| NotifyError("session cookie missing".to_owned()))
}

fn cookie_value(response: &HttpResponse, name: &str) -> Option<String> {
    response.headers.iter().find_map(|(key, value)| {
        if !key.eq_ignore_ascii_case("set-cookie") {
            return None;
        }
        let (cookie_name, cookie_value) = value.split_once('=')?;
        if cookie_name != name {
            return None;
        }
        Some(
            cookie_value
                .split(';')
                .next()
                .unwrap_or(cookie_value)
                .to_owned(),
        )
    })
}

fn http_request(
    method: &str,
    url: &Url,
    extra_headers: &[(&str, &str)],
    body: &[u8],
) -> Result<HttpResponse, NotifyError> {
    let addr = socket_addr_for_url(url).map_err(|error| NotifyError(error.0))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))
        .map_err(|error| NotifyError(error.to_string()))?;
    stream.set_read_timeout(Some(Duration::from_secs(2))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(2))).ok();
    let host = host_header(url);
    let path = if let Some(query) = url.query() {
        format!("{}?{query}", url.path())
    } else {
        url.path().to_owned()
    };
    let mut request = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    if !extra_headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"))
    {
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    for (name, value) in extra_headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.write_all(body))
        .map_err(|error| NotifyError(error.to_string()))?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|error| NotifyError(error.to_string()))?;
    parse_http_response(&buf)
}

fn parse_http_response(raw: &[u8]) -> Result<HttpResponse, NotifyError> {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .ok_or_else(|| NotifyError("truncated HTTP response".to_owned()))?;
    let mut lines = head.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| NotifyError("missing HTTP status".to_owned()))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| NotifyError(format!("invalid status line {status_line}")))?;
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_owned(), value.trim().to_owned()))
        .collect();
    Ok(HttpResponse {
        status,
        headers,
        body: decode_body(head, body),
    })
}

fn decode_body(head: &str, body: &str) -> String {
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        decode_chunked(body)
    } else {
        body.to_owned()
    }
}

fn decode_chunked(body: &str) -> String {
    let mut remaining = body;
    let mut out = String::new();
    while !remaining.is_empty() {
        let Some((size_line, rest)) = remaining.split_once("\r\n") else {
            out.push_str(remaining);
            break;
        };
        let Ok(size) = usize::from_str_radix(size_line.trim(), 16) else {
            out.push_str(remaining);
            break;
        };
        if size == 0 {
            break;
        }
        if rest.len() < size {
            out.push_str(rest);
            break;
        }
        out.push_str(&rest[..size]);
        remaining = rest[size..].trim_start_matches("\r\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;

    #[test]
    fn notification_text_fires_only_when_pending_count_rises() {
        assert_eq!(pending_approval_notification(0, 0), None);
        assert_eq!(
            pending_approval_notification(0, 1).as_deref(),
            Some("1 pending approval")
        );
        assert_eq!(
            pending_approval_notification(1, 4).as_deref(),
            Some("4 pending approvals")
        );
        assert_eq!(pending_approval_notification(4, 4), None);
        assert_eq!(pending_approval_notification(4, 2), None);
    }

    #[test]
    fn created_session_status_is_success() {
        assert!(http_success(201));
        assert!(http_success(200));
        assert!(!http_success(199));
        assert!(!http_success(400));
    }

    #[test]
    fn pending_count_reads_the_real_json_array() {
        assert_eq!(pending_approval_count_from_json("[]").expect("empty"), 0);
        assert_eq!(
            pending_approval_count_from_json(r#"[{"id":"a"},{"id":"b"}]"#).expect("two"),
            2
        );
        assert!(pending_approval_count_from_json("{}").is_err());
    }

    #[test]
    fn fetch_pending_count_uses_session_cookie_against_loopback_http() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let origin = accept_webview_url(&format!("http://{addr}")).expect("origin");
        std::thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept");
                let mut buf = [0_u8; 2048];
                let n = stream.read(&mut buf).unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]);
                let (status, headers, body) = if request.starts_with("POST /api/v1/session") {
                    (
                        201,
                        "Set-Cookie: harness_session=desktop-session; HttpOnly; Path=/\r\nContent-Type: application/json\r\n",
                        r#"{"csrf_token":"t","expires_at_ms":1}"#,
                    )
                } else if request.contains("Cookie: harness_session=desktop-session")
                    && request.starts_with("GET /api/v1/approvals?state=pending")
                {
                    (
                        200,
                        "Content-Type: application/json\r\n",
                        r#"[{"id":"one"},{"id":"two"}]"#,
                    )
                } else {
                    (400, "", r#"{"error":"bad"}"#)
                };
                let response = format!(
                    "HTTP/1.1 {status} OK\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
            }
        });
        assert_eq!(fetch_pending_approval_count(&origin).expect("count"), 2);
    }
}
