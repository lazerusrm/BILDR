use std::{
    collections::BTreeSet,
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

const SCHEMA_DIGEST_ENCODING: &str = "normalized-compact-json";

#[derive(Parser)]
#[command(name = "cargo xtask", version, about = "BILDR build tasks")]
struct Cli {
    #[command(subcommand)]
    command: Task,
}

#[derive(Subcommand)]
enum Task {
    UiInstall {
        #[arg(long)]
        locked: bool,
    },
    UiBuild,
    Check,
    OpenapiCheck,
    SchemaCheck,
    AppServerBindingsCheck,
    CodexSchema {
        #[arg(long, default_value = "codex")]
        codex: PathBuf,
    },
    Dist {
        #[arg(long)]
        check: bool,
    },
}

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("xtask has no workspace parent")?
        .to_path_buf();
    match Cli::parse().command {
        Task::UiInstall { locked } => ui_install(&root, locked),
        Task::UiBuild => ui_build(&root),
        Task::Check => check(&root),
        Task::OpenapiCheck => openapi_check(&root),
        Task::SchemaCheck => schema_check(&root),
        Task::AppServerBindingsCheck => app_server_bindings_check(&root),
        Task::CodexSchema { codex } => codex_schema(&root, &codex),
        Task::Dist { check } => dist(&root, check),
    }
}

fn ui_install(root: &Path, locked: bool) -> Result<()> {
    let ui = root.join("ui");
    let command = if locked {
        if !ui.join("package-lock.json").exists() {
            bail!("ui/package-lock.json is required for --locked")
        }
        "ci"
    } else {
        "install"
    };
    run(
        Command::new("npm").arg(command).current_dir(ui),
        "npm install",
    )
}

fn ui_build(root: &Path) -> Result<()> {
    if !root.join("ui/node_modules").exists() {
        ui_install(root, root.join("ui/package-lock.json").exists())?;
    }
    run(
        Command::new("npm")
            .args(["run", "build"])
            .current_dir(root.join("ui")),
        "UI build",
    )?;
    require_file(&root.join("ui/dist/index.html"))
}

fn check(root: &Path) -> Result<()> {
    schema_check(root)?;
    openapi_check(root)?;
    app_server_bindings_check(root)?;
    ui_build(root)?;
    run(
        Command::new("cargo")
            .args(["fmt", "--all", "--", "--check"])
            .current_dir(root),
        "rustfmt",
    )?;
    run(
        Command::new("cargo")
            .args(["test", "--workspace", "--all-targets"])
            .current_dir(root),
        "workspace tests",
    )
}

fn schema_check(root: &Path) -> Result<()> {
    let mut checked = 0_usize;
    for directory in [root.join("schemas"), root.join("examples")] {
        for entry in WalkDir::new(directory).into_iter().filter_map(Result::ok) {
            if entry.path().extension().and_then(|value| value.to_str()) != Some("json") {
                continue;
            }
            let _: Value = serde_json::from_slice(&fs::read(entry.path())?)
                .with_context(|| format!("invalid JSON: {}", entry.path().display()))?;
            checked += 1;
        }
    }
    if checked < 4 {
        bail!("expected schemas and examples, found only {checked} JSON files")
    }
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("config/harness.example.toml"),
    )?)?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("profiles/bildr/profile.toml"),
    )?)?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("profiles/general/profile.toml"),
    )?)?;
    println!("schema-check: {checked} JSON files and both TOML profiles parsed");
    Ok(())
}

fn openapi_check(root: &Path) -> Result<()> {
    let path = root.join("openapi/harness-api.yaml");
    let value: serde_yaml::Value = serde_yaml::from_slice(&fs::read(&path)?)?;
    let mapping = value
        .as_mapping()
        .context("OpenAPI document must be a mapping")?;
    if mapping.get("openapi").is_none() || mapping.get("paths").is_none() {
        bail!("OpenAPI document has no openapi or paths field")
    }
    let pointers = collect_refs(&value);
    for pointer in &pointers {
        let pointer = pointer
            .strip_prefix('#')
            .context("only local OpenAPI references are allowed")?;
        if yaml_pointer(&value, pointer).is_none() {
            bail!("unresolved OpenAPI reference #{pointer}")
        }
    }
    let documented_routes = mapping
        .get("paths")
        .and_then(serde_yaml::Value::as_mapping)
        .context("OpenAPI paths must be a mapping")?
        .keys()
        .map(|path| {
            path.as_str()
                .map(|path| format!("/api/v1{}", normalize_path_parameters(path)))
                .context("OpenAPI path keys must be strings")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let router_source = fs::read_to_string(root.join("crates/harness-api/src/lib.rs"))?;
    let implemented_routes = rust_router_paths(&router_source);
    let missing = documented_routes
        .difference(&implemented_routes)
        .cloned()
        .collect::<Vec<_>>();
    let undocumented = implemented_routes
        .difference(&documented_routes)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() || !undocumented.is_empty() {
        bail!(
            "OpenAPI/router path drift; missing implementations: {missing:?}; undocumented routes: {undocumented:?}"
        )
    }
    println!(
        "openapi-check: {} local references resolved; {} router paths match",
        pointers.len(),
        documented_routes.len()
    );
    Ok(())
}

fn rust_router_paths(source: &str) -> BTreeSet<String> {
    source
        .match_indices(".route(")
        .filter_map(|(offset, marker)| {
            let tail = source.get(offset + marker.len()..)?;
            let quote = tail.find('"')?;
            let value = tail.get(quote + 1..)?;
            let end = value.find('"')?;
            value.get(..end).map(ToOwned::to_owned)
        })
        .filter(|path| path.starts_with("/api/v1/"))
        .collect()
}

fn normalize_path_parameters(path: &str) -> String {
    let mut result = String::with_capacity(path.len());
    let mut parameter = false;
    for character in path.chars() {
        match character {
            '{' => {
                parameter = true;
                result.push(character);
            }
            '}' => {
                parameter = false;
                result.push(character);
            }
            uppercase if parameter && uppercase.is_ascii_uppercase() => {
                result.push('_');
                result.push(uppercase.to_ascii_lowercase());
            }
            other => result.push(other),
        }
    }
    result
}

fn collect_refs(value: &serde_yaml::Value) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            for (key, value) in mapping {
                if key.as_str() == Some("$ref")
                    && let Some(reference) = value.as_str()
                {
                    refs.insert(reference.to_owned());
                }
                refs.extend(collect_refs(value));
            }
        }
        serde_yaml::Value::Sequence(sequence) => {
            for value in sequence {
                refs.extend(collect_refs(value));
            }
        }
        _ => {}
    }
    refs
}

fn yaml_pointer<'a>(
    mut value: &'a serde_yaml::Value,
    pointer: &str,
) -> Option<&'a serde_yaml::Value> {
    if pointer.is_empty() {
        return Some(value);
    }
    for segment in pointer.trim_start_matches('/').split('/') {
        let segment = segment.replace("~1", "/").replace("~0", "~");
        value = value
            .as_mapping()?
            .get(serde_yaml::Value::String(segment))?;
    }
    Some(value)
}

fn app_server_bindings_check(root: &Path) -> Result<()> {
    let schema =
        root.join("generated/codex-app-server-schema/codex_app_server_protocol.v2.schemas.json");
    require_file(&schema)?;
    let schema_bytes = fs::read(&schema)?;
    let _: Value = serde_json::from_slice(&schema_bytes)?;
    let digest = canonical_json_sha256(&schema_bytes)?;
    let config: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("config/harness.example.toml"),
    )?)?;
    let configured = config
        .get("codex")
        .and_then(|value| value.get("required_protocol_schema_sha256"))
        .and_then(toml::Value::as_str)
        .context("config has no Codex schema digest")?;
    if configured != digest {
        bail!("generated App Server schema digest is {digest}, config pins {configured}")
    }
    let compatibility: Value =
        serde_json::from_slice(&fs::read(root.join("generated/CODEX_COMPATIBILITY.json"))?)?;
    if compatibility
        .get("root_schema_sha256_encoding")
        .and_then(Value::as_str)
        != Some(SCHEMA_DIGEST_ENCODING)
    {
        bail!("generated/CODEX_COMPATIBILITY.json has the wrong schema digest encoding")
    }
    if compatibility
        .get("root_schema_sha256")
        .and_then(Value::as_str)
        != Some(digest.as_str())
    {
        bail!("generated/CODEX_COMPATIBILITY.json does not match generated schema")
    }
    let configured_version = config
        .get("codex")
        .and_then(|value| value.get("required_version"))
        .and_then(toml::Value::as_str)
        .context("config has no required Codex version")?;
    if compatibility
        .get("codex_cli_version")
        .and_then(Value::as_str)
        != Some(configured_version)
    {
        bail!("generated/CODEX_COMPATIBILITY.json does not match configured Codex version")
    }
    println!("app-server-bindings-check: {digest}");
    Ok(())
}

fn codex_schema(root: &Path, codex: &Path) -> Result<()> {
    let temporary = tempfile::tempdir()?;
    run(
        Command::new(codex)
            .args(["app-server", "generate-json-schema", "--out"])
            .arg(temporary.path()),
        "Codex schema generation",
    )?;
    let source = temporary
        .path()
        .join("codex_app_server_protocol.v2.schemas.json");
    require_file(&source)?;
    let digest = canonical_json_sha256(&fs::read(&source)?)?;
    let destination = root.join("generated/codex-app-server-schema");
    if destination.exists() {
        fs::remove_dir_all(&destination)?;
    }
    fs::create_dir_all(&destination)?;
    for entry in WalkDir::new(temporary.path())
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
    {
        let relative = entry.path().strip_prefix(temporary.path())?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), target)?;
        }
    }
    let version = output_text(Command::new(codex).arg("--version"), "Codex version probe")?;
    fs::write(
        root.join("generated/CODEX_COMPATIBILITY.json"),
        serde_json::to_vec_pretty(&json!({
            "schema": "harness-codex-compatibility/v1",
            "codex_cli_version": version.split_whitespace().last().unwrap_or(&version),
            "transport": "stdio-jsonl",
            "generated_schema_root": "generated/codex-app-server-schema",
            "root_schema": "codex_app_server_protocol.v2.schemas.json",
            "root_schema_sha256": digest,
            "root_schema_sha256_encoding": SCHEMA_DIGEST_ENCODING,
            "generated_at": "update-with-release-metadata"
        }))?,
    )?;
    println!("generated schema {digest}; update the intentional pins in config after review");
    Ok(())
}

fn dist(root: &Path, check_only: bool) -> Result<()> {
    for path in [
        "ui/dist/index.html",
        "config/harness.example.toml",
        "profiles/general/profile.toml",
        "profiles/bildr/profile.toml",
        "packaging/systemd/harnessd.service",
        "packaging/desktop/bildr.desktop",
        "generated/CODEX_COMPATIBILITY.json",
        "LICENSE",
        "README.md",
        "VERSION",
    ] {
        require_file(&root.join(path))?;
    }
    if check_only {
        println!("dist --check: release inputs present");
        return Ok(());
    }
    ui_build(root)?;
    run(
        Command::new("cargo")
            .args([
                "build",
                "--release",
                "--package",
                "harnessd",
                "--package",
                "harnessctl",
                "--package",
                "harness-probe",
                "--package",
                "harness-desktop",
            ])
            .current_dir(root),
        "release build",
    )?;
    let version = fs::read_to_string(root.join("VERSION"))?.trim().to_owned();
    let dist = root.join("dist");
    let stage = dist.join(format!("bildr-{version}-linux-x86_64"));
    if stage.exists() {
        fs::remove_dir_all(&stage)?;
    }
    fs::create_dir_all(stage.join("bin"))?;
    fs::create_dir_all(stage.join("share/harness-console"))?;
    let target_dir = cargo_target_dir(root);
    for binary in ["harnessd", "harnessctl", "harness-probe", "harness-desktop"] {
        fs::copy(
            target_dir.join("release").join(binary),
            stage.join("bin").join(binary),
        )?;
    }
    for path in [
        "LICENSE",
        "README.md",
        "VERSION",
        "generated/CODEX_COMPATIBILITY.json",
        "openapi/harness-api.yaml",
        "packaging/systemd/harnessd.service",
        "packaging/desktop/bildr.desktop",
    ] {
        let destination = stage.join("share/harness-console").join(path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(root.join(path), destination)?;
    }
    for path in ["codex", "config", "profiles", "schemas"] {
        copy_tree(
            &root.join(path),
            &stage.join("share/harness-console").join(path),
        )?;
    }
    let archive = dist.join(format!("bildr-{version}-linux-x86_64.tar.gz"));
    run(
        Command::new("tar")
            .arg("-C")
            .arg(&dist)
            .args(["-czf"])
            .arg(&archive)
            .arg(stage.file_name().unwrap()),
        "distribution archive",
    )?;
    fs::write(
        PathBuf::from(format!("{}.sha256", archive.display())),
        format!(
            "{}  {}\n",
            sha256(&fs::read(&archive)?),
            archive.file_name().unwrap().to_string_lossy()
        ),
    )?;
    println!("dist: {}", archive.display());
    Ok(())
}

fn cargo_target_dir(root: &Path) -> PathBuf {
    match env::var_os("CARGO_TARGET_DIR") {
        Some(value) => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                path
            } else {
                root.join(path)
            }
        }
        None => root.join("target"),
    }
}

fn require_file(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("required file {} is missing", path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        bail!("required file {} is empty or not a file", path.display())
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    for entry in WalkDir::new(source) {
        let entry = entry?;
        let relative = entry.path().strip_prefix(source)?;
        let target = destination.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn run(command: &mut Command, label: &str) -> Result<()> {
    command.stdin(Stdio::null());
    let status = command
        .status()
        .with_context(|| format!("failed to start {label}"))?;
    if !status.success() {
        bail!("{label} failed with {status}")
    }
    Ok(())
}

fn output_text(command: &mut Command, label: &str) -> Result<String> {
    let output = command
        .output()
        .with_context(|| format!("failed to start {label}"))?;
    if !output.status.success() {
        bail!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn canonical_json_sha256(bytes: &[u8]) -> Result<String> {
    let value = normalize_json(serde_json::from_slice(bytes)?);
    Ok(sha256(&serde_json::to_vec(&value)?))
}

fn normalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut fields = map.into_iter().collect::<Vec<_>>();
            fields.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, normalize_json(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(normalize_json).collect()),
        value => value,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_json_digest_ignores_object_order() {
        let first = br#"{"version":1,"schema":{"type":"object","required":["id"]}}"#;
        let reordered = br#"{"schema":{"required":["id"],"type":"object"},"version":1}"#;

        assert_eq!(
            canonical_json_sha256(first).unwrap(),
            canonical_json_sha256(reordered).unwrap()
        );
    }
}
