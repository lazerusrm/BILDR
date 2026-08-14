//! Exact-SHA evidence records and self-verifying bundle exports.

mod investigation;

pub use investigation::InvestigationEvidenceService;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use harness_domain::{
    ArtifactId, AttemptId, EvidenceId, ProofTier, ResultClass, RunId, ValidationId,
};
use harness_store::{NewArtifact, NewEvidenceRecord, Store};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tar::{Archive, Builder, Header};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct EvidenceArtifactInput {
    pub path: PathBuf,
    pub kind: String,
    pub logical_name: String,
    pub media_type: String,
    pub sensitivity: String,
    pub purpose: String,
    pub retention_class: String,
}

#[derive(Clone, Debug)]
pub struct EvidenceClaim {
    pub id: EvidenceId,
    pub run_id: RunId,
    pub task_attempt_id: Option<AttemptId>,
    pub validation_id: Option<ValidationId>,
    pub claim_id: String,
    pub checklist_rows: Vec<String>,
    pub source_sha: String,
    pub proof_tier: ProofTier,
    pub result_class: ResultClass,
    pub details: Value,
    pub unproved_claims: Vec<String>,
    pub artifacts: Vec<EvidenceArtifactInput>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StoredEvidence {
    pub evidence_id: EvidenceId,
    pub evidence_sha256: String,
    pub artifacts: Vec<ArtifactId>,
}

#[derive(Clone)]
pub struct EvidenceService {
    store: Store,
}

impl EvidenceService {
    #[must_use]
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn record(&self, claim: EvidenceClaim) -> Result<StoredEvidence, EvidenceError> {
        validate_source_sha(&claim.source_sha)?;
        if claim.result_class == ResultClass::Success && !claim.unproved_claims.is_empty() {
            return Err(EvidenceError::Invalid(
                "successful evidence cannot contain unproved claims".to_owned(),
            ));
        }
        if claim.claim_id.trim().is_empty() {
            return Err(EvidenceError::Invalid(
                "claim id must not be empty".to_owned(),
            ));
        }

        let mut artifacts = Vec::new();
        let mut purposes = Vec::new();
        for input in &claim.artifacts {
            if !input.path.is_file() {
                return Err(EvidenceError::Invalid(format!(
                    "evidence artifact does not exist: {}",
                    input.path.display()
                )));
            }
            let stored = self.store.artifacts().put_file(&input.path)?;
            let artifact_id = self.store.register_artifact(&NewArtifact {
                id: ArtifactId::new(),
                run_id: Some(claim.run_id.clone()),
                task_attempt_id: claim.task_attempt_id.clone(),
                kind: input.kind.clone(),
                logical_name: input.logical_name.clone(),
                storage_path: stored.path,
                sha256: stored.digest,
                media_type: input.media_type.clone(),
                compression: None,
                sensitivity: input.sensitivity.clone(),
                byte_length: stored.byte_length,
                retention_class: input.retention_class.clone(),
                pinned: false,
            })?;
            purposes.push((artifact_id.clone(), input.purpose.clone()));
            artifacts.push(artifact_id);
        }

        let evidence_value = serde_json::json!({
            "schema": "harness-evidence/v1",
            "claim_id": claim.claim_id,
            "source_sha": claim.source_sha,
            "proof_tier": claim.proof_tier,
            "result_class": claim.result_class,
            "details": claim.details,
            "artifact_ids": artifacts,
        });
        let evidence_bytes = serde_json::to_vec(&evidence_value)?;
        let evidence_sha256 = digest(&evidence_bytes);
        self.store.record_evidence(&NewEvidenceRecord {
            id: claim.id.clone(),
            run_id: claim.run_id,
            task_attempt_id: claim.task_attempt_id,
            validation_id: claim.validation_id,
            claim_id: claim.claim_id,
            checklist_rows: claim.checklist_rows,
            source_sha: claim.source_sha,
            proof_tier: claim.proof_tier,
            result_class: claim.result_class,
            evidence: evidence_value,
            unproved_claims: claim.unproved_claims,
        })?;
        for (artifact_id, purpose) in purposes {
            self.store
                .link_evidence_artifact(&claim.id, &artifact_id, &purpose)?;
        }
        Ok(StoredEvidence {
            evidence_id: claim.id,
            evidence_sha256,
            artifacts,
        })
    }

    pub fn export_bundle(
        &self,
        run_id: &RunId,
        output: &Path,
    ) -> Result<BundleExport, EvidenceError> {
        let snapshot = self.store.evidence_snapshot(run_id)?;
        let snapshot_bytes = serde_json::to_vec_pretty(&snapshot)?;
        let mut payloads = BTreeMap::<String, Vec<u8>>::new();
        payloads.insert("snapshot.json".to_owned(), snapshot_bytes);
        if let Some(artifacts) = snapshot.get("artifacts").and_then(Value::as_array) {
            for artifact in artifacts {
                let sha = artifact
                    .get("sha256")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        EvidenceError::Invalid("artifact snapshot lacks sha256".to_owned())
                    })?;
                let bytes = self.store.artifacts().read(sha)?;
                payloads.insert(format!("artifacts/{sha}"), bytes);
            }
        }
        let entries = payloads
            .iter()
            .map(|(path, bytes)| BundleEntry {
                path: path.clone(),
                sha256: digest(bytes),
                bytes: bytes.len() as u64,
            })
            .collect();
        let manifest = BundleManifest {
            schema: "harness-evidence-bundle/v1".to_owned(),
            run_id: run_id.to_string(),
            entries,
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        let manifest_sha256 = digest(&manifest_bytes);

        if let Some(parent) = output.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = output.with_extension(format!(
            "tmp-{}-{}",
            std::process::id(),
            ulid::Ulid::generate()
        ));
        let file = File::create(&temporary)?;
        let encoder = zstd::stream::write::Encoder::new(file, 9)?;
        let mut builder = Builder::new(encoder);
        append_tar(&mut builder, "manifest.json", &manifest_bytes)?;
        for (path, bytes) in &payloads {
            append_tar(&mut builder, path, bytes)?;
        }
        let encoder = builder.into_inner()?;
        let file = encoder.finish()?;
        file.sync_all()?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
        fs::rename(&temporary, output)?;
        let bundle = self.store.artifacts().put_file(output)?;
        let artifact_id = self.store.register_artifact(&NewArtifact {
            id: ArtifactId::new(),
            run_id: Some(run_id.clone()),
            task_attempt_id: None,
            kind: "evidence_bundle".to_owned(),
            logical_name: output
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("evidence.tar.zst")
                .to_owned(),
            storage_path: bundle.path,
            sha256: bundle.digest.clone(),
            media_type: "application/zstd".to_owned(),
            compression: Some("zstd".to_owned()),
            sensitivity: "internal".to_owned(),
            byte_length: bundle.byte_length,
            retention_class: "release_evidence".to_owned(),
            pinned: true,
        })?;
        self.store
            .record_run_export(run_id, &artifact_id, &manifest_sha256)?;
        Ok(BundleExport {
            path: output.to_path_buf(),
            artifact_id,
            bundle_sha256: bundle.digest,
            manifest_sha256,
            entries: manifest.entries.len() as u64,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BundleEntry {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BundleManifest {
    pub schema: String,
    pub run_id: String,
    pub entries: Vec<BundleEntry>,
}

#[derive(Clone, Debug, Serialize)]
pub struct BundleExport {
    pub path: PathBuf,
    pub artifact_id: ArtifactId,
    pub bundle_sha256: String,
    pub manifest_sha256: String,
    pub entries: u64,
}

pub fn verify_bundle(path: &Path) -> Result<BundleManifest, EvidenceError> {
    let file = File::open(path)?;
    let decoder = zstd::stream::read::Decoder::new(file)?;
    let mut archive = Archive::new(decoder);
    let mut files = BTreeMap::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        if !entry.header().entry_type().is_file() {
            return Err(EvidenceError::Invalid(
                "bundle contains a non-regular entry".to_owned(),
            ));
        }
        let path = entry.path()?.to_string_lossy().into_owned();
        if !safe_bundle_path(&path) {
            return Err(EvidenceError::Invalid(format!(
                "unsafe bundle path: {path}"
            )));
        }
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        if files.insert(path.clone(), bytes).is_some() {
            return Err(EvidenceError::Invalid(format!(
                "duplicate bundle path: {path}"
            )));
        }
    }
    let manifest_bytes = files
        .remove("manifest.json")
        .ok_or_else(|| EvidenceError::Invalid("bundle has no manifest".to_owned()))?;
    let manifest: BundleManifest = serde_json::from_slice(&manifest_bytes)?;
    if manifest.schema != "harness-evidence-bundle/v1" {
        return Err(EvidenceError::Invalid(format!(
            "unsupported bundle schema {}",
            manifest.schema
        )));
    }
    let expected: BTreeSet<&str> = manifest
        .entries
        .iter()
        .map(|entry| entry.path.as_str())
        .collect();
    if expected.len() != manifest.entries.len()
        || manifest
            .entries
            .iter()
            .any(|entry| entry.path == "manifest.json" || !safe_bundle_path(&entry.path))
    {
        return Err(EvidenceError::Invalid(
            "bundle manifest contains duplicate or unsafe entry paths".to_owned(),
        ));
    }
    let actual: BTreeSet<&str> = files.keys().map(String::as_str).collect();
    if expected != actual {
        return Err(EvidenceError::Invalid(
            "bundle entry set does not match manifest".to_owned(),
        ));
    }
    for entry in &manifest.entries {
        let bytes = files.get(&entry.path).ok_or_else(|| {
            EvidenceError::Invalid(format!("missing bundle entry {}", entry.path))
        })?;
        if bytes.len() as u64 != entry.bytes || digest(bytes) != entry.sha256 {
            return Err(EvidenceError::Invalid(format!(
                "bundle entry failed integrity check: {}",
                entry.path
            )));
        }
    }
    Ok(manifest)
}

fn append_tar<W: Write>(
    builder: &mut Builder<W>,
    path: &str,
    bytes: &[u8],
) -> Result<(), EvidenceError> {
    let mut header = Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o600);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_cksum();
    builder.append_data(&mut header, path, bytes)?;
    Ok(())
}

fn validate_source_sha(value: &str) -> Result<(), EvidenceError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(EvidenceError::Invalid(
            "source SHA must be an exact lowercase 40-digit Git SHA".to_owned(),
        ));
    }
    Ok(())
}

fn safe_bundle_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.split('/').any(|part| part.is_empty() || part == "..")
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Debug, Error)]
pub enum EvidenceError {
    #[error("store error: {0}")]
    Store(#[from] harness_store::StoreError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid evidence: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn invalid_or_uppercase_source_sha_is_rejected() {
        assert!(validate_source_sha("abc").is_err());
        assert!(validate_source_sha(&"A".repeat(40)).is_err());
        assert!(validate_source_sha(&"a".repeat(40)).is_ok());
    }

    #[test]
    fn bundle_verifier_detects_valid_archive() {
        let temp = TempDir::new().unwrap();
        let output = temp.path().join("fixture.tar.zst");
        let payload = b"proof";
        let manifest = BundleManifest {
            schema: "harness-evidence-bundle/v1".to_owned(),
            run_id: "run".to_owned(),
            entries: vec![BundleEntry {
                path: "snapshot.json".to_owned(),
                sha256: digest(payload),
                bytes: payload.len() as u64,
            }],
        };
        let file = File::create(&output).unwrap();
        let encoder = zstd::stream::write::Encoder::new(file, 1).unwrap();
        let mut builder = Builder::new(encoder);
        append_tar(
            &mut builder,
            "manifest.json",
            &serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        append_tar(&mut builder, "snapshot.json", payload).unwrap();
        let encoder = builder.into_inner().unwrap();
        encoder.finish().unwrap();
        assert_eq!(verify_bundle(&output).unwrap().run_id, "run");
    }
}
