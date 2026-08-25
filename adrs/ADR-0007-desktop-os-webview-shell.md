# ADR-0007: Native desktop shell is a Tauri 2 / wry OS-webview around harnessd

**Status:** accepted

## Decision

Ship `harness-desktop` as a Tauri 2 application whose window is the OS webview
(wry: WebKitGTK, WKWebView, WebView2). The window loads the existing harnessd
loopback HTTP UI so the session cookie and CSRF contract stay unchanged.

The desktop process is a shell: it probes loopback, starts harnessd as a
separate process when it is down, and attaches when it is up. It does not link
the orchestrator and does not expose Git, Codex, SQLite, or run-approval IPC.
Tray, native notifications, the folder picker, and `bildr://` / `harness://`
openers live in the Rust core so they work under the operator UI CSP.

## Rationale

Electron would add a bundled Chromium. A from-scratch egui/Iced/GPUI console
would throw away the operator UI. A thin OS-webview around harnessd reuses the
control plane, keeps recovery in the daemon, and adds desktop affordances.

## Consequences

- REST/SSE remains the operator mutation path;
- Tauri capabilities grant only shell-class permissions;
- Linux systemd remains the service manager; the shell may start a sidecar;
- browser/PWA and SSH-forwarded loopback access stay supported.
