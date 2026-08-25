use std::{io::Write, path::PathBuf, process::Child, sync::Mutex, thread, time::Duration};

use anyhow::{Context, Result};
use tauri::menu::{Menu, MenuItem};
use tauri::tray::{MouseButton, TrayIconBuilder, TrayIconEvent};
use tauri::webview::{PageLoadEvent, PageLoadPayload};
use tauri::{AppHandle, Manager, WebviewUrl, WebviewWindow, WebviewWindowBuilder, WindowEvent};
use tauri_plugin_dialog::DialogExt;
use tauri_plugin_notification::NotificationExt;
use url::Url;

use crate::capabilities::{IPC_COMMANDS, WEBVIEW_ENGINE};
use crate::notify::{fetch_pending_approval_count, pending_approval_notification};
use crate::opener::{OpenerAction, new_project_query_url, parse_opener, register_query_url};
use crate::options::DesktopOptions;
use crate::origin::{accept_webview_url, desktop_shell_url};
use crate::sidecar::{
    SidecarSpec, execute_daemon_plan, plan_daemon_lifecycle, resolve_harnessd, wait_for_health,
};
use crate::webview_env::apply_os_webview_workarounds;

const WINDOW_LABEL: &str = "main";

struct ShellState {
    origin: Url,
    sidecar: Mutex<Option<Child>>,
    own_sidecar: bool,
}

pub fn run() -> Result<()> {
    apply_os_webview_workarounds();
    let options = DesktopOptions::from_args();
    println!("webview engine: {WEBVIEW_ENGINE}");
    let _ = std::io::stdout().flush();

    let origin = accept_webview_url(&options.url).context("desktop origin")?;
    let spec = SidecarSpec {
        program: resolve_harnessd(options.harnessd.as_deref()),
        inspection_only: options.without_codex,
    };
    let plan = plan_daemon_lifecycle(&origin, &spec).context("daemon lifecycle")?;
    println!("{}", plan.report());
    let _ = std::io::stdout().flush();

    let child = execute_daemon_plan(&plan).context("sidecar spawn")?;
    if !plan.is_attach() {
        println!("waiting for harnessd health at {}", plan.url());
        let _ = std::io::stdout().flush();
        wait_for_health(plan.url(), Duration::from_secs(30)).context("harnessd health")?;
    }

    let mut url = desktop_shell_url(plan.url());
    let mut pick_on_open = None;
    if let Some(opener) = &options.opener {
        match parse_opener(opener).context("opener")? {
            OpenerAction::ShowWindow => {}
            OpenerAction::PickFolder { new_project } => pick_on_open = Some(new_project),
            OpenerAction::Register { path } => {
                url = register_query_url(&url, &path).context("register opener")?;
            }
            OpenerAction::NewProject { parent_path } => {
                url = new_project_query_url(&url, &parent_path).context("new project opener")?;
            }
        }
    }

    let state = ShellState {
        origin: url.clone(),
        sidecar: Mutex::new(child),
        own_sidecar: options.own_sidecar,
    };

    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_notification::init())
        .invoke_handler(tauri::generate_handler![
            show_window,
            pick_repository_folder
        ])
        .manage(state)
        .setup(move |app| {
            let _ = IPC_COMMANDS;
            install_tray(app.handle())?;
            open_operator_window(app.handle(), url.clone())?;
            if let Some(new_project) = pick_on_open {
                pick_folder_and_navigate(app.handle(), new_project);
            }
            start_approval_poller(app.handle().clone());
            Ok(())
        })
        .build(tauri::generate_context!())
        .context("tauri runtime")?
        .run(|app, event| {
            if let tauri::RunEvent::Exit = event {
                stop_owned_sidecar(app);
            }
        });
    Ok(())
}

fn open_operator_window(app: &AppHandle, url: Url) -> tauri::Result<WebviewWindow> {
    let handle = app.clone();
    let window = WebviewWindowBuilder::new(app, WINDOW_LABEL, WebviewUrl::External(url.clone()))
        .title("BILDR")
        .inner_size(1440.0, 900.0)
        .min_inner_size(1180.0, 720.0)
        .on_navigation({
            let handle = handle.clone();
            move |target| navigation_allowed(&handle, target)
        })
        .on_page_load(|_window, payload: PageLoadPayload<'_>| {
            if payload.event() == PageLoadEvent::Finished {
                println!("webview navigated to {}", payload.url());
                let _ = std::io::stdout().flush();
            }
        })
        .build()?;

    let hidden = window.clone();
    window.on_window_event(move |event| {
        if let WindowEvent::CloseRequested { api, .. } = event {
            api.prevent_close();
            let _ = hidden.hide();
        }
    });
    Ok(window)
}

fn navigation_allowed(app: &AppHandle, url: &Url) -> bool {
    if url.scheme() == "bildr" || url.scheme() == "harness" {
        match parse_opener(url.as_str()) {
            Ok(OpenerAction::ShowWindow) => {
                let _ = reveal_window(app);
            }
            Ok(OpenerAction::PickFolder { new_project }) => {
                pick_folder_and_navigate(app, new_project)
            }
            Ok(OpenerAction::Register { path }) => navigate_register(app, &path),
            Ok(OpenerAction::NewProject { parent_path }) => navigate_new_project(app, &parent_path),
            Err(error) => eprintln!("opener rejected: {error}"),
        }
        return false;
    }
    match accept_webview_url(url.as_str()) {
        Ok(accepted) => {
            println!("webview navigated to {accepted}");
            let _ = std::io::stdout().flush();
            true
        }
        Err(error) => {
            eprintln!("blocked webview navigation to {url}: {error}");
            false
        }
    }
}

fn install_tray(app: &AppHandle) -> tauri::Result<()> {
    let show = MenuItem::with_id(app, "show", "Show BILDR", true, None::<&str>)?;
    let register = MenuItem::with_id(app, "register", "Register repository…", true, None::<&str>)?;
    let new_project =
        MenuItem::with_id(app, "new-project", "New local project…", true, None::<&str>)?;
    let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
    let menu = Menu::with_items(app, &[&show, &register, &new_project, &quit])?;
    let mut tray = TrayIconBuilder::with_id("bildr")
        .menu(&menu)
        .tooltip("BILDR")
        .show_menu_on_left_click(true)
        .on_menu_event(|app, event| match event.id.as_ref() {
            "show" => {
                let _ = reveal_window(app);
            }
            "register" => pick_folder_and_navigate(app, false),
            "new-project" => pick_folder_and_navigate(app, true),
            "quit" => app.exit(0),
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            if let TrayIconEvent::Click {
                button: MouseButton::Left,
                ..
            } = event
            {
                let _ = reveal_window(tray.app_handle());
            }
        });
    if let Some(icon) = app.default_window_icon() {
        tray = tray.icon(icon.clone());
    }
    tray.build(app)?;
    Ok(())
}

fn start_approval_poller(app: AppHandle) {
    thread::Builder::new()
        .name("bildr-approval-notify".into())
        .spawn(move || {
            let mut previous = 0_usize;
            loop {
                thread::sleep(Duration::from_secs(12));
                let origin = app
                    .try_state::<ShellState>()
                    .map(|state| state.origin.clone());
                let Some(origin) = origin else {
                    continue;
                };
                if let Ok(current) = fetch_pending_approval_count(&origin) {
                    if let Some(body) = pending_approval_notification(previous, current) {
                        let handle = app.clone();
                        let _ = handle.clone().run_on_main_thread(move || {
                            show_approval_notification(&handle, &body);
                        });
                    }
                    previous = current;
                }
            }
        })
        .ok();
}

fn show_approval_notification(app: &AppHandle, body: &str) {
    if let Err(error) = app
        .notification()
        .builder()
        .title("BILDR")
        .body(body)
        .show()
    {
        eprintln!("approval notification failed: {error}");
    }
}

fn pick_folder_and_navigate(app: &AppHandle, new_project: bool) {
    match pick_folder_path(app, new_project) {
        Ok(Some(path)) if new_project => navigate_new_project(app, &PathBuf::from(path)),
        Ok(Some(path)) => navigate_register(app, &PathBuf::from(path)),
        Ok(None) => {}
        Err(error) => eprintln!("folder picker failed: {error}"),
    }
}

fn pick_folder_path(app: &AppHandle, new_project: bool) -> Result<Option<String>, String> {
    let picked = app
        .dialog()
        .file()
        .set_title(if new_project {
            "Choose parent folder for new project"
        } else {
            "Register a repository"
        })
        .blocking_pick_folder();
    Ok(picked.map(|path| path.to_string()))
}

fn navigate_register(app: &AppHandle, folder: &std::path::Path) {
    let Some(state) = app.try_state::<ShellState>() else {
        return;
    };
    match register_query_url(&state.origin, folder) {
        Ok(url) => {
            if let Some(window) = app.get_webview_window(WINDOW_LABEL)
                && let Err(error) = window.navigate(url.clone())
            {
                eprintln!("failed to open register flow: {error}");
            }
            let _ = reveal_window(app);
        }
        Err(error) => eprintln!("register URL rejected: {error}"),
    }
}

fn navigate_new_project(app: &AppHandle, parent: &std::path::Path) {
    let Some(state) = app.try_state::<ShellState>() else {
        return;
    };
    match new_project_query_url(&state.origin, parent) {
        Ok(url) => {
            if let Some(window) = app.get_webview_window(WINDOW_LABEL)
                && let Err(error) = window.navigate(url.clone())
            {
                eprintln!("failed to open new-project flow: {error}");
            }
            let _ = reveal_window(app);
        }
        Err(error) => eprintln!("new-project URL rejected: {error}"),
    }
}

fn reveal_window(app: &AppHandle) -> Result<(), String> {
    let Some(window) = app.get_webview_window(WINDOW_LABEL) else {
        return Err("main window missing".to_owned());
    };
    window.unminimize().map_err(|error| error.to_string())?;
    window.show().map_err(|error| error.to_string())?;
    window.set_focus().map_err(|error| error.to_string())?;
    Ok(())
}

fn stop_owned_sidecar(app: &AppHandle) {
    let Some(state) = app.try_state::<ShellState>() else {
        return;
    };
    if !state.own_sidecar {
        return;
    }
    if let Ok(mut child) = state.sidecar.lock()
        && let Some(child) = child.as_mut()
    {
        let _ = child.kill();
    }
}

#[tauri::command]
fn show_window(app: AppHandle) -> Result<(), String> {
    reveal_window(&app)
}

#[tauri::command]
fn pick_repository_folder(app: AppHandle) -> Result<Option<String>, String> {
    pick_folder_path(&app, false)
}
