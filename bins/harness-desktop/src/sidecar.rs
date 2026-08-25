use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use url::Url;

use crate::origin::{OriginError, accept_webview_url, bind_address, host_header, loopback_ip};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct SidecarError(pub String);

impl From<OriginError> for SidecarError {
    fn from(error: OriginError) -> Self {
        Self(error.0)
    }
}

impl From<std::io::Error> for SidecarError {
    fn from(error: std::io::Error) -> Self {
        Self(error.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SidecarSpec {
    pub program: PathBuf,
    pub inspection_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DaemonPlan {
    Attach {
        url: Url,
    },
    Start {
        url: Url,
        program: PathBuf,
        args: Vec<String>,
    },
}

impl DaemonPlan {
    pub fn url(&self) -> &Url {
        match self {
            Self::Attach { url } | Self::Start { url, .. } => url,
        }
    }

    pub fn is_attach(&self) -> bool {
        matches!(self, Self::Attach { .. })
    }

    pub fn report(&self) -> String {
        match self {
            Self::Attach { url } => {
                format!("daemon lifecycle: attach-only {url}; not spawning harnessd")
            }
            Self::Start { url, program, args } => format!(
                "daemon lifecycle: start {} {} for {url}",
                program.display(),
                args.join(" ")
            ),
        }
    }
}

pub fn resolve_harnessd(explicit: Option<&Path>) -> PathBuf {
    if let Some(path) = explicit
        && !path.as_os_str().is_empty()
    {
        return path.to_path_buf();
    }
    if let Ok(path) = std::env::var("HARNESSD") {
        let path = path.trim();
        if !path.is_empty() {
            return PathBuf::from(path);
        }
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let candidate = dir.join(harnessd_filename());
        if candidate.is_file() {
            return candidate;
        }
    }
    PathBuf::from(harnessd_filename())
}

fn harnessd_filename() -> &'static str {
    if cfg!(windows) {
        "harnessd.exe"
    } else {
        "harnessd"
    }
}

pub fn harnessd_serve_args(url: &Url, inspection_only: bool) -> Result<Vec<String>, SidecarError> {
    let bind = bind_address(url)?;
    let mut args = vec![
        "serve".to_owned(),
        "--no-browser".to_owned(),
        "--bind".to_owned(),
        bind,
    ];
    if inspection_only {
        args.push("--without-codex".to_owned());
    }
    Ok(args)
}

pub fn loopback_port_is_open(url: &Url) -> Result<bool, SidecarError> {
    let _ = accept_webview_url(url.as_str())?;
    let addr = socket_addr_for_url(url)?;
    Ok(TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok())
}

pub fn socket_addr_for_url(url: &Url) -> Result<SocketAddr, SidecarError> {
    let port = url
        .port_or_known_default()
        .ok_or_else(|| SidecarError("webview URL is missing a port".to_owned()))?;
    let ip = loopback_ip(url)
        .ok_or_else(|| SidecarError("webview URL host is not loopback".to_owned()))?;
    Ok(SocketAddr::from((ip, port)))
}

pub fn plan_daemon_lifecycle(url: &Url, spec: &SidecarSpec) -> Result<DaemonPlan, SidecarError> {
    let url = accept_webview_url(url.as_str())?;
    if loopback_port_is_open(&url)? {
        return Ok(DaemonPlan::Attach { url });
    }
    Ok(DaemonPlan::Start {
        args: harnessd_serve_args(&url, spec.inspection_only)?,
        program: spec.program.clone(),
        url,
    })
}

pub fn execute_daemon_plan(plan: &DaemonPlan) -> Result<Option<Child>, SidecarError> {
    match plan {
        DaemonPlan::Attach { .. } => Ok(None),
        DaemonPlan::Start { program, args, .. } => {
            if looks_like_orchestrator_invocation(args) {
                return Err(SidecarError(
                    "refusing to spawn a harness mutation command from the desktop shell"
                        .to_owned(),
                ));
            }
            let child = Command::new(program)
                .args(args)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .map_err(|error| {
                    SidecarError(format!(
                        "failed to start sidecar {}: {error}",
                        program.display()
                    ))
                })?;
            Ok(Some(child))
        }
    }
}

pub fn looks_like_orchestrator_invocation(args: &[String]) -> bool {
    const FORBIDDEN: &[&str] = &[
        "create-run",
        "create_run",
        "approve-plan",
        "approve_plan",
        "orchestrat",
        "git",
        "sqlite",
        "codex-exec",
        "merge",
    ];
    args.iter().any(|arg| {
        let lowered = arg.to_ascii_lowercase();
        FORBIDDEN
            .iter()
            .any(|token| lowered == *token || lowered.contains(token))
    })
}

pub fn wait_for_health(url: &Url, timeout: Duration) -> Result<(), SidecarError> {
    let deadline = Instant::now() + timeout;
    let mut last = "harnessd did not become healthy".to_owned();
    while Instant::now() < deadline {
        match probe_health(url) {
            Ok(()) => return Ok(()),
            Err(error) => last = error.0,
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    Err(SidecarError(last))
}

pub fn probe_health(url: &Url) -> Result<(), SidecarError> {
    let url = accept_webview_url(url.as_str())?;
    let addr = socket_addr_for_url(&url)?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(400))?;
    stream.set_read_timeout(Some(Duration::from_millis(800)))?;
    stream.set_write_timeout(Some(Duration::from_millis(800)))?;
    let host = host_header(&url);
    let request = format!(
        "GET /api/v1/health HTTP/1.1\r\nHost: {host}\r\nAccept: application/json\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes())?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let text = String::from_utf8_lossy(&buf);
    let status_ok = text.starts_with("HTTP/1.1 200") || text.starts_with("HTTP/1.0 200");
    if status_ok {
        return Ok(());
    }
    Err(SidecarError(format!(
        "health probe failed: {}",
        text.chars().take(180).collect::<String>()
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;

    fn ephemeral_origin(listener: &TcpListener) -> Url {
        let addr = listener.local_addr().expect("listener address");
        accept_webview_url(&format!("http://{addr}")).expect("loopback bind")
    }

    #[test]
    fn attach_when_loopback_listener_is_already_up() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
        let url = ephemeral_origin(&listener);
        let spec = SidecarSpec {
            program: PathBuf::from("/usr/bin/false"),
            inspection_only: true,
        };
        let plan = plan_daemon_lifecycle(&url, &spec).expect("plan");
        assert!(plan.is_attach(), "{:?}", plan.report());
        assert_eq!(plan.url(), &url);
        assert!(
            execute_daemon_plan(&plan)
                .expect("execute attach")
                .is_none()
        );
        assert!(!plan.report().contains("start "));
        drop(listener);
    }

    #[test]
    fn start_when_loopback_listener_is_down_emits_harnessd_serve() {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("reserve port")
            .local_addr()
            .expect("port")
            .port();
        let url = accept_webview_url(&format!("http://127.0.0.1:{port}")).expect("url");
        let spec = SidecarSpec {
            program: PathBuf::from("/opt/bildr/harnessd"),
            inspection_only: true,
        };
        let plan = plan_daemon_lifecycle(&url, &spec).expect("plan");
        match plan {
            DaemonPlan::Start {
                program,
                args,
                url: planned,
            } => {
                assert_eq!(program, spec.program);
                assert_eq!(planned, url);
                assert_eq!(args.first().map(String::as_str), Some("serve"));
                assert!(args.iter().any(|arg| arg == "--no-browser"));
                assert!(args.iter().any(|arg| arg == "--without-codex"));
                assert!(args.iter().any(|arg| arg == "--bind"));
                assert!(args.iter().any(|arg| arg == &format!("127.0.0.1:{port}")));
                assert!(!looks_like_orchestrator_invocation(&args));
                assert!(!args.iter().any(|arg| arg.contains("create_run")));
                assert!(!args.iter().any(|arg| arg.contains("approve_plan")));
            }
            DaemonPlan::Attach { url } => panic!("expected start, attached to {url}"),
        }
    }

    #[test]
    fn wait_for_health_succeeds_against_real_loopback_http() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("health listener");
        let url = ephemeral_origin(&listener);
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept health");
            let mut buf = [0_u8; 512];
            let _ = stream.read(&mut buf);
            let body = br#"{"status":"ok"}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                std::str::from_utf8(body).expect("body")
            );
            let _ = stream.write_all(response.as_bytes());
        });
        wait_for_health(&url, Duration::from_secs(2)).expect("health");
    }

    #[test]
    fn resolve_harnessd_prefers_explicit_path() {
        let path = PathBuf::from("/tmp/explicit-harnessd");
        assert_eq!(resolve_harnessd(Some(&path)), path);
    }
}
