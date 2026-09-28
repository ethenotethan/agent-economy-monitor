use std::{
    error::Error,
    fmt,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_primitives::fs::open_dir_nofollow;
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use sha2::{Digest, Sha256};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceContext {
    source: String,
    observed_date: String,
}

impl EvidenceContext {
    pub fn new(source: &str, observed_date: &str) -> Result<Self, StoreError> {
        if !valid_source(source) {
            return Err(StoreError::InvalidContext("source"));
        }
        if !valid_date(observed_date) {
            return Err(StoreError::InvalidContext("observed_date"));
        }
        Ok(Self {
            source: source.to_owned(),
            observed_date: observed_date.to_owned(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceObject {
    name: String,
    digest: [u8; 32],
}

impl EvidenceObject {
    pub fn parse(name: &str) -> Result<Self, StoreError> {
        let parts = name.split('/').collect::<Vec<_>>();
        if parts.len() != 6
            || parts[0] != "evidence"
            || !valid_source(parts[1])
            || !valid_date(parts[2])
            || parts[3] != "sha256"
            || parts[4].len() != 2
            || parts[5].len() != 64
            || !parts[5]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || parts[4] != &parts[5][..2]
        {
            return Err(StoreError::InvalidObjectName);
        }
        let digest = parse_hex_digest(parts[5]).ok_or(StoreError::InvalidObjectName)?;
        Ok(Self {
            name: name.to_owned(),
            digest,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sha256(&self) -> String {
        hex_digest(&self.digest)
    }

    fn for_bytes(context: &EvidenceContext, bytes: &[u8]) -> Self {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let digest_hex = hex_digest(&digest);
        Self {
            name: format!(
                "evidence/{}/{}/sha256/{}/{}",
                context.source,
                context.observed_date,
                &digest_hex[..2],
                digest_hex
            ),
            digest,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDisposition {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateReceipt {
    pub object: EvidenceObject,
    pub disposition: CreateDisposition,
}

pub trait EvidenceStore {
    fn create(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError>;

    fn read(&self, object: &EvidenceObject) -> Result<Vec<u8>, StoreError>;
}

#[derive(Clone, Debug)]
pub struct FilesystemEvidenceStore {
    root: Arc<Dir>,
}

impl FilesystemEvidenceStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = if root.as_ref().is_absolute() {
            root.as_ref().to_owned()
        } else {
            std::env::current_dir()?.join(root)
        };
        let anchor = root
            .ancestors()
            .last()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or(StoreError::InvalidObjectPath)?;
        let relative = root
            .strip_prefix(anchor)
            .map_err(|_| StoreError::InvalidObjectPath)?;
        let mut current = Dir::open_ambient_dir(anchor, ambient_authority())?;
        let mut logical_path = anchor.to_owned();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                return Err(StoreError::UnsafePath { path: root });
            };
            logical_path.push(component);
            current = open_or_create_directory(&current, component, &logical_path)?;
        }
        Ok(Self {
            root: Arc::new(current),
        })
    }

    fn secure_parent(&self, object: &EvidenceObject) -> Result<Dir, StoreError> {
        let relative_parent = Path::new(object.name())
            .parent()
            .ok_or(StoreError::InvalidObjectPath)?;
        let mut current = self.root.try_clone()?;
        let mut logical_path = PathBuf::new();
        for component in relative_parent.components() {
            let Component::Normal(component) = component else {
                return Err(StoreError::UnsafePath {
                    path: relative_parent.to_owned(),
                });
            };
            logical_path.push(component);
            current = open_or_create_directory(&current, component, &logical_path)?;
        }
        Ok(current)
    }

    fn secure_path(&self, object: &EvidenceObject) -> Result<(Dir, PathBuf), StoreError> {
        let parent = self.secure_parent(object)?;
        let file_name = Path::new(object.name())
            .file_name()
            .ok_or(StoreError::InvalidObjectPath)?;
        Ok((parent, file_name.into()))
    }

    fn read_verified(&self, object: &EvidenceObject) -> Result<Vec<u8>, StoreError> {
        let (parent, file_name) = self.secure_path(object)?;
        let mut file = open_regular_file_nofollow(&parent, &file_name)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != object.digest {
            return Err(StoreError::DigestMismatch {
                object: object.name.clone(),
                expected: object.sha256(),
                actual: hex_digest(&actual),
            });
        }
        Ok(bytes)
    }
}

impl EvidenceStore for FilesystemEvidenceStore {
    fn create(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError> {
        let object = EvidenceObject::for_bytes(context, evidence);
        let parent = self.secure_parent(&object)?;
        let file_name = Path::new(object.name())
            .file_name()
            .ok_or(StoreError::InvalidObjectPath)?;

        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary_path = format!(".evidence-{}-{sequence}.tmp", std::process::id());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut temporary = parent.open_with(&temporary_path, &options)?;
        temporary.write_all(evidence)?;
        temporary.sync_all()?;
        drop(temporary);

        let disposition = match parent.hard_link(&temporary_path, &parent, file_name) {
            Ok(()) => {
                sync_directory(&parent)?;
                CreateDisposition::Created
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                self.read_verified(&object)?;
                CreateDisposition::AlreadyPresent
            }
            Err(error) => {
                let _ = parent.remove_file(&temporary_path);
                return Err(error.into());
            }
        };
        parent.remove_file(&temporary_path)?;
        sync_directory(&parent)?;

        Ok(CreateReceipt {
            object,
            disposition,
        })
    }

    fn read(&self, object: &EvidenceObject) -> Result<Vec<u8>, StoreError> {
        self.read_verified(object)
    }
}

#[derive(Debug)]
pub enum StoreError {
    InvalidContext(&'static str),
    InvalidObjectName,
    InvalidObjectPath,
    UnsafePath {
        path: PathBuf,
    },
    DigestMismatch {
        object: String,
        expected: String,
        actual: String,
    },
    Io(std::io::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContext(field) => write!(formatter, "invalid evidence {field}"),
            Self::InvalidObjectName => formatter.write_str("invalid evidence object name"),
            Self::InvalidObjectPath => formatter.write_str("invalid evidence object path"),
            Self::UnsafePath { path } => {
                write!(formatter, "unsafe evidence path: {}", path.display())
            }
            Self::DigestMismatch { object, .. } => {
                write!(formatter, "evidence digest mismatch for {object}")
            }
            Self::Io(error) => write!(formatter, "evidence storage I/O failed: {error}"),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn valid_source(source: &str) -> bool {
    !source.is_empty()
        && source.len() <= 64
        && source.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
        && source
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && source
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_date(date: &str) -> bool {
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| !matches!(index, 4 | 7) && !byte.is_ascii_digit())
    {
        return false;
    }
    let month = date[5..7].parse::<u8>().ok();
    let day = date[8..10].parse::<u8>().ok();
    matches!(month, Some(1..=12)) && matches!(day, Some(1..=31))
}

fn hex_digest(digest: &[u8; 32]) -> String {
    use fmt::Write as _;

    let mut value = String::with_capacity(64);
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn parse_hex_digest(value: &str) -> Option<[u8; 32]> {
    let mut digest = [0_u8; 32];
    for (index, output) in digest.iter_mut().enumerate() {
        let offset = index * 2;
        *output = u8::from_str_radix(&value[offset..offset + 2], 16).ok()?;
    }
    if hex_digest(&digest) == value {
        Some(digest)
    } else {
        None
    }
}

fn open_or_create_directory(
    parent: &Dir,
    component: &std::ffi::OsStr,
    logical_path: &Path,
) -> Result<Dir, StoreError> {
    match open_directory_nofollow(parent, component, logical_path) {
        Ok(directory) => Ok(directory),
        Err(StoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            match parent.create_dir(component) {
                Ok(()) => sync_directory(parent)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            open_directory_nofollow(parent, component, logical_path)
        }
        Err(error) => Err(error),
    }
}

fn open_directory_nofollow(
    parent: &Dir,
    component: &std::ffi::OsStr,
    logical_path: &Path,
) -> Result<Dir, StoreError> {
    let parent_file = parent.try_clone()?.into_std_file();
    open_dir_nofollow(&parent_file, Path::new(component))
        .map(Dir::from_std_file)
        .map_err(|error| {
            if matches!(
                parent.symlink_metadata(component),
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir()
            ) {
                StoreError::UnsafePath {
                    path: logical_path.to_owned(),
                }
            } else {
                error.into()
            }
        })
}

fn open_regular_file_nofollow(directory: &Dir, path: &Path) -> std::io::Result<cap_std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_fs_ext::OpenOptionsExt;

        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = directory.open_with(path, &options)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "evidence object is not a regular file",
        ));
    }
    Ok(file)
}

fn sync_directory(directory: &Dir) -> Result<(), StoreError> {
    #[cfg(unix)]
    directory.open(".")?.sync_all()?;
    #[cfg(not(unix))]
    directory.try_clone()?.into_std_file().sync_all()?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, os::unix::fs::symlink, process::Command, sync::mpsc, thread, time::Duration};

    use super::*;

    #[test]
    fn final_object_symlink_is_never_followed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = fs::canonicalize(temporary.path()).expect("canonical temporary root");
        let outside = root
            .parent()
            .expect("temporary parent")
            .join("outside-evidence");
        fs::write(&outside, b"outside").expect("write outside fixture");
        symlink(&outside, root.join("object")).expect("install final symlink");
        let directory = Dir::open_ambient_dir(&root, ambient_authority()).expect("open root");

        let error = open_regular_file_nofollow(&directory, Path::new("object"))
            .expect_err("final symlink must fail closed");

        assert_ne!(error.kind(), std::io::ErrorKind::NotFound);
        fs::remove_file(outside).expect("remove outside fixture");
    }

    #[test]
    fn final_object_fifo_is_rejected_promptly() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = fs::canonicalize(temporary.path()).expect("canonical temporary root");
        let fifo = root.join("object");
        assert!(
            Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .expect("run mkfifo")
                .success()
        );
        let directory = Dir::open_ambient_dir(&root, ambient_authority()).expect("open root");
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let result = open_regular_file_nofollow(&directory, Path::new("object"));
            let _ = sender.send(result.map(|_| ()));
        });

        let result = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("FIFO rejection must not block");
        assert!(result.is_err());
    }
}
