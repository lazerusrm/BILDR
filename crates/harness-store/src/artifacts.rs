use std::{
    fs::{self, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use sha2::{Digest, Sha256};

use crate::StoreError;

static NEXT_TEMP_FILE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
pub struct ArtifactStore {
    root: Arc<PathBuf>,
}

impl ArtifactStore {
    pub fn new(root: &Path) -> Result<Self, StoreError> {
        fs::create_dir_all(root)?;
        fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            root: Arc::new(root.to_path_buf()),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    pub fn put(&self, bytes: &[u8]) -> Result<StoredArtifact, StoreError> {
        let digest = hex::encode(Sha256::digest(bytes));
        let path = self.path_for(&digest)?;
        if path.exists() {
            self.verify(&digest)?;
            return Ok(StoredArtifact {
                digest,
                path,
                byte_length: bytes.len() as u64,
            });
        }
        let parent = path.parent().ok_or_else(|| {
            StoreError::ArtifactIntegrity("artifact path has no parent".to_owned())
        })?;
        fs::create_dir_all(parent)?;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        let temporary = parent.join(format!(
            ".{}.tmp-{}-{}",
            digest,
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        match fs::hard_link(&temporary, &path) {
            Ok(()) => fs::remove_file(&temporary)?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                fs::remove_file(&temporary)?;
                self.verify(&digest)?;
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error.into());
            }
        }
        if let Ok(directory) = OpenOptions::new().read(true).open(parent) {
            let _ = directory.sync_all();
        }
        Ok(StoredArtifact {
            digest,
            path,
            byte_length: bytes.len() as u64,
        })
    }

    pub fn put_file(&self, source: &Path) -> Result<StoredArtifact, StoreError> {
        let mut input = OpenOptions::new().read(true).open(source)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        let mut byte_length = 0_u64;
        loop {
            let count = input.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            byte_length = byte_length.saturating_add(count as u64);
        }
        let digest = hex::encode(hasher.finalize());
        let path = self.path_for(&digest)?;
        if path.exists() {
            self.verify(&digest)?;
            return Ok(StoredArtifact {
                digest,
                path,
                byte_length,
            });
        }
        let parent = path.parent().ok_or_else(|| {
            StoreError::ArtifactIntegrity("artifact path has no parent".to_owned())
        })?;
        fs::create_dir_all(parent)?;
        fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        let temporary = parent.join(format!(
            ".{}.tmp-{}-{}",
            digest,
            std::process::id(),
            NEXT_TEMP_FILE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::copy(source, &temporary)?;
        fs::set_permissions(&temporary, fs::Permissions::from_mode(0o600))?;
        OpenOptions::new().read(true).open(&temporary)?.sync_all()?;
        match fs::hard_link(&temporary, &path) {
            Ok(()) => fs::remove_file(&temporary)?,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                fs::remove_file(&temporary)?;
                if self.verify(&digest)? != byte_length {
                    return Err(StoreError::ArtifactIntegrity(format!(
                        "concurrent artifact {} has unexpected length",
                        digest
                    )));
                }
                return Ok(StoredArtifact {
                    digest,
                    path,
                    byte_length,
                });
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error.into());
            }
        }
        let actual_length = match self.verify(&digest) {
            Ok(length) => length,
            Err(error) => {
                let _ = fs::remove_file(&path);
                return Err(error);
            }
        };
        if actual_length != byte_length {
            let _ = fs::remove_file(&path);
            return Err(StoreError::ArtifactIntegrity(format!(
                "copied artifact {} changed during ingestion",
                digest
            )));
        }
        Ok(StoredArtifact {
            digest,
            path,
            byte_length,
        })
    }

    pub fn read(&self, digest: &str) -> Result<Vec<u8>, StoreError> {
        self.verify_digest(digest)?;
        Ok(fs::read(self.path_for(digest)?)?)
    }

    pub fn verify(&self, digest: &str) -> Result<u64, StoreError> {
        self.verify_digest(digest)?;
        let path = self.path_for(digest)?;
        let mut file = OpenOptions::new().read(true).open(&path)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 64 * 1024];
        let mut length = 0_u64;
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
            length = length.saturating_add(count as u64);
        }
        let actual = hex::encode(hasher.finalize());
        if actual != digest {
            return Err(StoreError::ArtifactIntegrity(format!(
                "expected {digest}, observed {actual} at {}",
                path.display()
            )));
        }
        Ok(length)
    }

    pub fn path_for(&self, digest: &str) -> Result<PathBuf, StoreError> {
        self.verify_digest(digest)?;
        Ok(self
            .root
            .join(&digest[0..2])
            .join(&digest[2..4])
            .join(digest))
    }

    fn verify_digest(&self, digest: &str) -> Result<(), StoreError> {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        {
            return Err(StoreError::ArtifactIntegrity(
                "artifact digest is not a SHA-256 hex string".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StoredArtifact {
    pub digest: String,
    pub path: PathBuf,
    pub byte_length: u64,
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn content_is_deduplicated_and_verified() {
        let temp = TempDir::new().unwrap();
        let store = ArtifactStore::new(temp.path()).unwrap();
        let first = store.put(b"evidence").unwrap();
        let second = store.put(b"evidence").unwrap();
        assert_eq!(first.digest, second.digest);
        assert_eq!(store.read(&first.digest).unwrap(), b"evidence");
    }
}
