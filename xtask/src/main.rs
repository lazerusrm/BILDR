use std::{
    collections::{BTreeMap, BTreeSet},
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
const JSON_SCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
const IMPROVEMENT_CRATES: &[&str] = &[
    "harness-trace",
    "harness-eval",
    "harness-learning",
    "harness-promotion",
];
// This applies only to the new improvement surfaces.  The legacy orchestrator
// and App are reviewed exceptions, rather than files subject to a growth cap.
const IMPROVEMENT_RUST_FILE_LINE_BUDGET: usize = 1_200;
const IMPROVEMENT_UI_FILE_LINE_BUDGET: usize = 1_200;

#[derive(Clone, Debug)]
struct SchemaDocument {
    path: PathBuf,
    value: Value,
}

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
    ArchitecturePolicyCheck,
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
        Task::ArchitecturePolicyCheck => architecture_policy_check(&root),
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
    architecture_policy_check(root)?;
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

fn architecture_policy_check(root: &Path) -> Result<()> {
    let mut violations = Vec::new();
    let crates_root = root.join("crates");
    for crate_name in IMPROVEMENT_CRATES {
        let crate_root = crates_root.join(crate_name);
        let manifest = crate_root.join("Cargo.toml");
        if !manifest.exists() {
            continue;
        }
        let value: toml::Value = toml::from_str(&fs::read_to_string(&manifest)?)
            .with_context(|| format!("invalid manifest: {}", manifest.display()))?;
        if manifest_depends_on_orchestrator(&value) {
            violations.push(format!(
                "{} must not depend on harness-orchestrator",
                manifest.display()
            ));
        }
        violations.extend(source_line_budget_violations(
            &crate_root,
            &["rs"],
            IMPROVEMENT_RUST_FILE_LINE_BUDGET,
        )?);
    }
    violations.extend(source_line_budget_violations(
        &root.join("ui/src/improvement"),
        &["ts", "tsx", "css"],
        IMPROVEMENT_UI_FILE_LINE_BUDGET,
    )?);
    if !violations.is_empty() {
        bail!(
            "architecture policy violations:\n- {}",
            violations.join("\n- ")
        )
    }
    println!(
        "architecture-policy-check: present improvement crates avoid harness-orchestrator; new improvement source files are within the {IMPROVEMENT_RUST_FILE_LINE_BUDGET}-line budget"
    );
    Ok(())
}

fn manifest_depends_on_orchestrator(manifest: &toml::Value) -> bool {
    dependency_tables(manifest).any(|dependencies| {
        dependencies.iter().any(|(name, specification)| {
            name == "harness-orchestrator"
                || specification
                    .as_table()
                    .and_then(|table| table.get("package"))
                    .and_then(toml::Value::as_str)
                    == Some("harness-orchestrator")
        })
    })
}

fn dependency_tables(
    manifest: &toml::Value,
) -> impl Iterator<Item = &toml::map::Map<String, toml::Value>> {
    let root = manifest.as_table();
    let direct = root.into_iter().flat_map(|table| {
        ["dependencies", "dev-dependencies", "build-dependencies"]
            .into_iter()
            .filter_map(|name| table.get(name).and_then(toml::Value::as_table))
    });
    let target = root
        .and_then(|table| table.get("target"))
        .and_then(toml::Value::as_table)
        .into_iter()
        .flat_map(|targets| targets.values())
        .filter_map(toml::Value::as_table)
        .flat_map(|table| {
            ["dependencies", "dev-dependencies", "build-dependencies"]
                .into_iter()
                .filter_map(|name| table.get(name).and_then(toml::Value::as_table))
        });
    direct.chain(target)
}

fn source_line_budget_violations(
    directory: &Path,
    extensions: &[&str],
    maximum_lines: usize,
) -> Result<Vec<String>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let entries = WalkDir::new(directory)
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("cannot walk source directory: {}", directory.display()))?;
    let mut paths = entries
        .into_iter()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .path()
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| extensions.contains(&extension))
        })
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    paths.sort();
    let mut violations = Vec::new();
    for path in paths {
        let lines = fs::read_to_string(&path)
            .with_context(|| format!("cannot read source file: {}", path.display()))?
            .lines()
            .count();
        if lines > maximum_lines {
            violations.push(format!(
                "{} has {lines} lines; the new improvement-source budget is {maximum_lines}",
                path.display()
            ));
        }
    }
    Ok(violations)
}

fn schema_check(root: &Path) -> Result<()> {
    let schemas = load_schema_catalog(&root.join("schemas"))?;
    let registry = schema_registry(&schemas)?;
    for schema in schemas.values() {
        compile_schema(&schema.path, &schema.value, &registry)?;
    }
    let examples = validate_schema_examples(&root.join("examples"), &schemas, &registry)?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("config/harness.example.toml"),
    )?)?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("profiles/bildr/profile.toml"),
    )?)?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("profiles/general/profile.toml"),
    )?)?;
    println!(
        "schema-check: {} Draft 2020-12 schemas and {examples} examples conform; config and profiles parsed",
        schemas.len()
    );
    Ok(())
}

fn load_schema_catalog(directory: &Path) -> Result<BTreeMap<String, SchemaDocument>> {
    let mut documents = BTreeMap::new();
    let mut ids = BTreeMap::<String, PathBuf>::new();
    for path in json_paths(directory)? {
        let value = read_json(&path)?;
        if value.get("$schema").and_then(Value::as_str) != Some(JSON_SCHEMA_2020_12) {
            bail!(
                "schema {} must declare {JSON_SCHEMA_2020_12}",
                path.display()
            )
        }
        let id = required_string(&value, "$id", &path)?;
        if let Some(first) = ids.insert(id.to_owned(), path.clone()) {
            bail!(
                "duplicate schema $id {id} in {} and {}",
                first.display(),
                path.display()
            )
        }
        let discriminator = value
            .pointer("/properties/schema/const")
            .and_then(Value::as_str)
            .with_context(|| {
                format!(
                    "schema {} has no string properties.schema.const discriminator",
                    path.display()
                )
            })?;
        let discriminator = discriminator.to_owned();
        if let Some(first) = documents.insert(
            discriminator.clone(),
            SchemaDocument {
                path: path.clone(),
                value,
            },
        ) {
            bail!(
                "duplicate schema discriminator {discriminator} in {} and {}",
                first.path.display(),
                path.display()
            )
        }
    }
    if documents.is_empty() {
        bail!("no JSON schemas found under {}", directory.display())
    }
    Ok(documents)
}

fn schema_registry(schemas: &BTreeMap<String, SchemaDocument>) -> Result<jsonschema::Registry<'_>> {
    let mut registry = jsonschema::Registry::new();
    for schema in schemas.values() {
        let id = required_string(&schema.value, "$id", &schema.path)?;
        registry = registry
            .add(id, &schema.value)
            .with_context(|| format!("invalid schema $id {id} in {}", schema.path.display()))?;
    }
    registry
        .prepare()
        .context("failed to prepare local JSON Schema registry")
}

fn validate_schema_examples(
    directory: &Path,
    schemas: &BTreeMap<String, SchemaDocument>,
    registry: &jsonschema::Registry<'_>,
) -> Result<usize> {
    let openapi_examples = directory.join("openapi");
    let paths = json_paths(directory)?
        .into_iter()
        .filter(|path| !path.starts_with(&openapi_examples))
        .collect::<Vec<_>>();
    if paths.is_empty() {
        bail!("no JSON examples found under {}", directory.display())
    }
    for path in &paths {
        let value = read_json(path)?;
        validate_schema_example(path, &value, schemas, registry)?;
    }
    Ok(paths.len())
}

fn validate_schema_example(
    path: &Path,
    value: &Value,
    schemas: &BTreeMap<String, SchemaDocument>,
    registry: &jsonschema::Registry<'_>,
) -> Result<()> {
    let discriminator = required_string(value, "schema", path)?;
    let schema = schemas.get(discriminator).with_context(|| {
        format!(
            "example {} names undocumented schema {discriminator}",
            path.display()
        )
    })?;
    let validator = compile_schema(&schema.path, &schema.value, registry)?;
    if let Err(error) = validator.validate(value) {
        bail!(
            "example {} does not conform to {}: {error}",
            path.display(),
            schema.path.display()
        )
    }
    Ok(())
}

fn compile_schema(
    path: &Path,
    value: &Value,
    registry: &jsonschema::Registry<'_>,
) -> Result<jsonschema::Validator> {
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .with_registry(registry)
        .should_validate_formats(true)
        .build(value)
        .with_context(|| format!("invalid Draft 2020-12 schema: {}", path.display()))
}

fn required_string<'a>(value: &'a Value, key: &str, path: &Path) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{} has no non-empty {key}", path.display()))
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("invalid JSON: {}", path.display()))
}

fn json_paths(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in WalkDir::new(directory) {
        let entry = entry.with_context(|| format!("cannot walk {}", directory.display()))?;
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|value| value.to_str()) == Some("json")
        {
            paths.push(entry.into_path());
        }
    }
    paths.sort();
    Ok(paths)
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
    let runtime_status_schema = runtime_status_schema(&value)?;
    let runtime_status_fixture =
        read_json(&root.join("examples/openapi/runtime-status.example.json"))?;
    let registry = jsonschema::Registry::new()
        .prepare()
        .context("failed to prepare OpenAPI JSON Schema registry")?;
    let runtime_status_validator = compile_schema(&path, &runtime_status_schema, &registry)?;
    if let Err(error) = runtime_status_validator.validate(&runtime_status_fixture) {
        bail!("RuntimeStatus fixture does not conform to OpenAPI: {error}")
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
        "openapi-check: {} local references resolved; RuntimeStatus fixture conforms; {} router paths match",
        pointers.len(),
        documented_routes.len()
    );
    Ok(())
}

fn runtime_status_schema(openapi: &serde_yaml::Value) -> Result<Value> {
    let mut schema = serde_json::to_value(openapi)
        .context("OpenAPI document cannot be represented as JSON Schema input")?;
    let object = schema
        .as_object_mut()
        .context("OpenAPI JSON Schema input must be an object")?;
    object.insert(
        "$schema".to_owned(),
        Value::String(JSON_SCHEMA_2020_12.to_owned()),
    );
    object.insert(
        "$ref".to_owned(),
        Value::String("#/components/schemas/RuntimeStatus".to_owned()),
    );
    Ok(schema)
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
    for binary in ["harnessd", "harnessctl", "harness-probe"] {
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
    use serde_json::json;

    #[test]
    fn canonical_json_digest_ignores_object_order() {
        let first = br#"{"version":1,"schema":{"type":"object","required":["id"]}}"#;
        let reordered = br#"{"schema":{"required":["id"],"type":"object"},"version":1}"#;

        assert_eq!(
            canonical_json_sha256(first).unwrap(),
            canonical_json_sha256(reordered).unwrap()
        );
    }

    #[test]
    fn schema_compilation_rejects_malformed_keywords_and_references() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let malformed = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:malformed",
            "type": 42
        });
        assert!(compile_schema(Path::new("malformed.json"), &malformed, &registry).is_err());

        let unresolved = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:unresolved",
            "$ref": "#/$defs/missing"
        });
        assert!(compile_schema(Path::new("unresolved.json"), &unresolved, &registry).is_err());
    }

    #[test]
    fn schema_catalog_rejects_missing_or_duplicate_identity() {
        let missing_id = tempfile::tempdir().unwrap();
        fs::write(
            missing_id.path().join("schema.json"),
            json!({
                "$schema": JSON_SCHEMA_2020_12,
                "properties": {"schema": {"const": "harness.example.v1"}}
            })
            .to_string(),
        )
        .unwrap();
        assert!(load_schema_catalog(missing_id.path()).is_err());

        let missing_discriminator = tempfile::tempdir().unwrap();
        fs::write(
            missing_discriminator.path().join("schema.json"),
            json!({
                "$schema": JSON_SCHEMA_2020_12,
                "$id": "urn:harness:missing-discriminator"
            })
            .to_string(),
        )
        .unwrap();
        assert!(load_schema_catalog(missing_discriminator.path()).is_err());

        let duplicate_id = tempfile::tempdir().unwrap();
        for (name, discriminator) in [
            ("first", "harness.first.v1"),
            ("second", "harness.second.v1"),
        ] {
            fs::write(
                duplicate_id.path().join(format!("{name}.json")),
                json!({
                    "$schema": JSON_SCHEMA_2020_12,
                    "$id": "urn:harness:duplicate",
                    "properties": {"schema": {"const": discriminator}}
                })
                .to_string(),
            )
            .unwrap();
        }
        assert!(load_schema_catalog(duplicate_id.path()).is_err());

        let duplicate_discriminator = tempfile::tempdir().unwrap();
        for name in ["first", "second"] {
            fs::write(
                duplicate_discriminator.path().join(format!("{name}.json")),
                json!({
                    "$schema": JSON_SCHEMA_2020_12,
                    "$id": format!("urn:harness:{name}"),
                    "properties": {"schema": {"const": "harness.example.v1"}}
                })
                .to_string(),
            )
            .unwrap();
        }
        assert!(load_schema_catalog(duplicate_discriminator.path()).is_err());
    }

    #[test]
    fn schema_compilation_resolves_catalog_references() {
        let root = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:root",
            "type": "object",
            "properties": {"value": {"$ref": "urn:harness:target"}}
        });
        let target =
            json!({"$schema": JSON_SCHEMA_2020_12, "$id": "urn:harness:target", "type": "string"});
        let catalog = BTreeMap::from([
            (
                "harness.root.v1".to_owned(),
                SchemaDocument {
                    path: PathBuf::from("root.json"),
                    value: root,
                },
            ),
            (
                "harness.target.v1".to_owned(),
                SchemaDocument {
                    path: PathBuf::from("target.json"),
                    value: target,
                },
            ),
        ]);
        let registry = schema_registry(&catalog).unwrap();
        let validator = compile_schema(
            &catalog["harness.root.v1"].path,
            &catalog["harness.root.v1"].value,
            &registry,
        )
        .unwrap();
        assert!(validator.is_valid(&json!({"value": "resolved"})));
        assert!(!validator.is_valid(&json!({"value": 42})));
    }

    #[test]
    fn candidate_schema_enforces_component_risk_pairings() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value = serde_json::from_str(include_str!(
            "../../schemas/harness.improvement-candidate.v1.schema.json"
        ))
        .unwrap();
        let validator =
            compile_schema(Path::new("candidate.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/candidate.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));

        let mut role_prompts_green = example.clone();
        role_prompts_green["edits"][0]["component_id"] = json!("role_prompts");
        assert!(!validator.is_valid(&role_prompts_green));

        let mut underclassified = example.clone();
        underclassified["edits"][0]["component_id"] = json!("role_prompts");
        underclassified["edits"][0]["risk_class"] = json!("amber");
        assert!(!validator.is_valid(&underclassified));

        for component_id in ["frozen_safety_anchor", "unknown_component"] {
            let mut forbidden = example.clone();
            forbidden["edits"][0]["component_id"] = json!(component_id);
            assert!(!validator.is_valid(&forbidden));
        }
    }

    #[test]
    fn outcome_schema_enforces_closed_manual_label_pairs() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value =
            serde_json::from_str(include_str!("../../schemas/harness.outcome.v1.schema.json"))
                .unwrap();
        let validator =
            compile_schema(Path::new("outcome.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/outcome.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));

        let mut acceptance_wrong_pair = example.clone();
        acceptance_wrong_pair["classification"] = json!("negative");
        acceptance_wrong_pair["code"] = json!("accepted_after_correction");
        assert!(!validator.is_valid(&acceptance_wrong_pair));

        let mut review_wrong_code = example.clone();
        review_wrong_code["dimension"] = json!("review_regression");
        review_wrong_code["classification"] = json!("negative");
        review_wrong_code["code"] = json!("arbitrary");
        assert!(!validator.is_valid(&review_wrong_code));

        let mut rollback_wrong_pair = example;
        rollback_wrong_pair["dimension"] = json!("rollback");
        rollback_wrong_pair["classification"] = json!("positive");
        rollback_wrong_pair["code"] = json!("rollback_recorded");
        assert!(!validator.is_valid(&rollback_wrong_pair));

        let mut automated_wrong_pair = rollback_wrong_pair;
        automated_wrong_pair["dimension"] = json!("ci_required_checks");
        automated_wrong_pair["classification"] = json!("positive");
        automated_wrong_pair["code"] = json!("failed");
        automated_wrong_pair["confidence"] = json!("authoritative");
        automated_wrong_pair["source"]["kind"] = json!("validation");
        assert!(!validator.is_valid(&automated_wrong_pair));
    }

    #[test]
    fn taskset_schema_rejects_open_split_and_extra_case_fields() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value =
            serde_json::from_str(include_str!("../../schemas/harness.taskset.v1.schema.json"))
                .unwrap();
        let validator =
            compile_schema(Path::new("taskset.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/taskset.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));
        let mut split = example.clone();
        split["cases"][0]["split"] = json!("unreviewed");
        assert!(!validator.is_valid(&split));
        let mut open = example;
        open["cases"][0]["answer"] = json!("secret");
        assert!(!validator.is_valid(&open));
    }

    #[test]
    fn example_validation_rejects_unknown_discriminators_and_extra_fields() {
        let schema_path = PathBuf::from("shape.schema.json");
        let schema = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:shape.v1",
            "type": "object",
            "additionalProperties": false,
            "required": ["schema", "value"],
            "properties": {
                "schema": {"const": "harness.shape.v1"},
                "value": {"type": "string"}
            }
        });
        let catalog = BTreeMap::from([(
            "harness.shape.v1".to_owned(),
            SchemaDocument {
                path: schema_path,
                value: schema,
            },
        )]);
        let registry = schema_registry(&catalog).unwrap();

        assert!(
            validate_schema_example(
                Path::new("unknown.json"),
                &json!({"schema": "harness.shape.v2", "value": "ok"}),
                &catalog,
                &registry,
            )
            .is_err()
        );
        assert!(
            validate_schema_example(
                Path::new("extra.json"),
                &json!({"schema": "harness.shape.v1", "value": "ok", "extra": true}),
                &catalog,
                &registry,
            )
            .is_err()
        );
    }

    #[test]
    fn architecture_policy_detects_direct_renamed_and_target_orchestrator_dependencies() {
        let allowed: toml::Value =
            toml::from_str("[dependencies]\nharness-domain = { path = \"../harness-domain\" }")
                .unwrap();
        assert!(!manifest_depends_on_orchestrator(&allowed));

        for manifest in [
            "[dependencies]\nharness-orchestrator = { path = \"../harness-orchestrator\" }",
            "[dependencies]\ncontroller = { package = \"harness-orchestrator\", path = \"../harness-orchestrator\" }",
            "[target.'cfg(unix)'.dev-dependencies]\nharness-orchestrator = { path = \"../harness-orchestrator\" }",
        ] {
            let value: toml::Value = toml::from_str(manifest).unwrap();
            assert!(manifest_depends_on_orchestrator(&value), "{manifest}");
        }
    }

    #[test]
    fn architecture_policy_enforces_only_the_new_source_roots_line_budget() {
        let root = tempfile::tempdir().unwrap();
        let trace = root.path().join("crates/harness-trace/src");
        let improvement = root.path().join("ui/src/improvement");
        let legacy = root.path().join("crates/harness-orchestrator/src");
        fs::create_dir_all(&trace).unwrap();
        fs::create_dir_all(&improvement).unwrap();
        fs::create_dir_all(&legacy).unwrap();
        fs::write(trace.join("within.rs"), "one\ntwo\n").unwrap();
        fs::write(trace.join("over.rs"), "one\ntwo\nthree\n").unwrap();
        fs::write(improvement.join("over.tsx"), "one\ntwo\nthree\n").unwrap();
        fs::write(legacy.join("legacy.rs"), "one\ntwo\nthree\nfour\n").unwrap();

        let rust =
            source_line_budget_violations(&root.path().join("crates/harness-trace"), &["rs"], 2)
                .unwrap();
        let ui = source_line_budget_violations(
            &root.path().join("ui/src/improvement"),
            &["ts", "tsx", "css"],
            2,
        )
        .unwrap();
        assert_eq!(rust.len(), 1);
        assert!(rust[0].contains("over.rs"));
        assert_eq!(ui.len(), 1);
        assert!(ui[0].contains("over.tsx"));
    }
}
