/// Environment WebKitGTK reads before the first webview is created.
///
/// NVIDIA + GBM currently fails with "Failed to create GBM buffer" and paints
/// a black surface. Disabling the dmabuf renderer and compositing falls back
/// to a shared-memory path that still uses the OS webview.
#[cfg(target_os = "linux")]
pub fn os_webview_env() -> &'static [(&'static str, &'static str)] {
    &[
        ("WEBKIT_DISABLE_DMABUF_RENDERER", "1"),
        ("WEBKIT_DISABLE_COMPOSITING_MODE", "1"),
    ]
}

#[cfg(not(target_os = "linux"))]
pub fn os_webview_env() -> &'static [(&'static str, &'static str)] {
    &[]
}

pub fn apply_os_webview_workarounds() {
    for (key, value) in os_webview_env() {
        if std::env::var_os(key).is_none() {
            // Called once at process start, before WebKit or worker threads start.
            unsafe { std::env::set_var(key, value) };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_guards_disable_webkit_dmabuf() {
        let keys: Vec<&str> = os_webview_env().iter().map(|(key, _)| *key).collect();
        assert!(
            keys.contains(&"WEBKIT_DISABLE_DMABUF_RENDERER"),
            "linux webview env must disable the NVIDIA GBM/dmabuf path: {keys:?}"
        );
        assert!(
            keys.contains(&"WEBKIT_DISABLE_COMPOSITING_MODE"),
            "linux webview env must disable compositing: {keys:?}"
        );
        for (_, value) in os_webview_env() {
            assert_eq!(*value, "1");
        }
    }
}
