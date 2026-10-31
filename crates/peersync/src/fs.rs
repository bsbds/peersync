use crate::{
    Comparison, IssueOperation, Limits, Resource, SymlinkMode, SyncError, SyncIssue, SyncOptions,
    filter::ripgrep::{self, SourceFilter},
    protocol::invalid,
};
use filetime::FileTime;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    ffi::OsString,
    fs,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

/// Components are byte arrays on Unix, UTF-8 on Windows. This is not a lossy
/// display string; unrepresentable cross-platform names fail explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct WirePath(pub Vec<Vec<u8>>);
impl WirePath {
    pub fn from_path(path: &Path) -> Result<Self, SyncError> {
        let mut result = Vec::new();
        for part in path.components() {
            if let Component::Normal(name) = part {
                result.push(os_bytes(name)?);
            } else {
                return Err(invalid("expected relative path components"));
            }
        }
        Ok(Self(result))
    }
    pub fn path(&self) -> Result<PathBuf, SyncError> {
        if self.0.len() > 256 {
            return Err(invalid("path too deep"));
        }
        let mut path = PathBuf::new();
        for raw in &self.0 {
            if raw.is_empty()
                || raw.len() > 4096
                || raw.contains(&0)
                || raw.contains(&b'/')
                || raw == b"."
                || raw == b".."
            {
                return Err(invalid("invalid relative path component"));
            }
            #[cfg(windows)]
            if raw.contains(&b'\\')
                || raw.contains(&b':')
                || raw.ends_with(b".")
                || raw.ends_with(b" ")
            {
                return Err(invalid("unsupported Windows path component"));
            }
            let name = bytes_os(raw.clone())?;
            let mut parts = Path::new(&name).components();
            if !matches!(parts.next(), Some(Component::Normal(_))) || parts.next().is_some() {
                return Err(invalid("invalid component"));
            }
            path.push(name);
        }
        Ok(path)
    }
}
#[cfg(unix)]
fn os_bytes(value: &std::ffi::OsStr) -> Result<Vec<u8>, SyncError> {
    use std::os::unix::ffi::OsStrExt;
    Ok(value.as_bytes().to_vec())
}
#[cfg(not(unix))]
fn os_bytes(value: &std::ffi::OsStr) -> Result<Vec<u8>, SyncError> {
    value
        .to_str()
        .map(|s| s.as_bytes().to_vec())
        .ok_or_else(|| invalid("unrepresentable file name"))
}
#[cfg(unix)]
fn bytes_os(value: Vec<u8>) -> Result<OsString, SyncError> {
    use std::os::unix::ffi::OsStringExt;
    Ok(OsString::from_vec(value))
}
#[cfg(not(unix))]
fn bytes_os(value: Vec<u8>) -> Result<OsString, SyncError> {
    String::from_utf8(value)
        .map(OsString::from)
        .map_err(|_| invalid("non-UTF8 path unsupported on this platform"))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Kind {
    Directory,
    File,
    Symlink,
    Skipped,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Entry {
    pub path: WirePath,
    pub kind: Kind,
    pub len: u64,
    pub mtime: i64,
    pub nanos: u32,
    pub mode: u32,
    pub digest: Option<String>,
    pub link: Option<Vec<u8>>,
}
pub(crate) struct Manifest {
    pub entries: Vec<Entry>,
    pub filter: SourceFilter,
    pub issues: Vec<SyncIssue>,
    pub failed: u64,
    pub error_directories: HashSet<WirePath>,
}

fn error_path(error: &ignore::Error) -> Option<&Path> {
    match error {
        ignore::Error::WithPath { path, .. } => Some(path),
        ignore::Error::WithDepth { err, .. } | ignore::Error::WithLineNumber { err, .. } => {
            error_path(err)
        }
        _ => None,
    }
}

pub(crate) fn absolute(path: &Path) -> Result<PathBuf, SyncError> {
    Ok(std::path::absolute(path)?)
}
pub(crate) fn check_cancel(cancel: &CancellationToken) -> Result<(), SyncError> {
    if cancel.is_cancelled() {
        Err(SyncError::Cancelled)
    } else {
        Ok(())
    }
}
pub(crate) fn scan(
    root: &Path,
    options: &SyncOptions,
    limits: &Limits,
    cancel: &CancellationToken,
) -> Result<Manifest, SyncError> {
    if !fs::metadata(root)?.is_dir() {
        return Err(invalid("source must be a directory"));
    }
    let builder = ripgrep::builder(root, &options.filter)?;
    let mut out = Manifest {
        entries: Vec::new(),
        filter: SourceFilter::new(root, &options.filter, limits)?,
        issues: Vec::new(),
        failed: 0,
        error_directories: HashSet::new(),
    };
    let mut bytes = 0usize;
    for item in builder.build() {
        check_cancel(cancel)?;
        let item = match item {
            Ok(item) => item,
            Err(e) => {
                out.failed += 1;
                let path = error_path(&e)
                    .and_then(|p| p.strip_prefix(root).ok())
                    .unwrap_or(Path::new(""));
                let directory = if out.entries.iter().any(|entry| {
                    entry.kind == Kind::Directory && entry.path.path().is_ok_and(|p| p == path)
                }) {
                    path
                } else {
                    path.parent().unwrap_or(Path::new(""))
                };
                out.error_directories
                    .insert(WirePath::from_path(directory)?);
                if out.error_directories.len() > limits.max_entries {
                    return Err(SyncError::LimitExceeded {
                        resource: Resource::Entries,
                    });
                }
                if out.issues.len() < limits.max_reported_issues {
                    out.issues.push(SyncIssue {
                        path: Some(path.to_owned()),
                        operation: IssueOperation::Scan,
                        message: e.to_string(),
                    });
                }
                continue;
            }
        };
        if let Some(e) = item.error() {
            return Err(invalid(format!("invalid ignore rules: {e}")));
        }
        let path = item.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|_| invalid("scan path outside root"))?;
        let info = match if relative.as_os_str().is_empty() {
            fs::metadata(path)
        } else {
            fs::symlink_metadata(path)
        } {
            Ok(m) => m,
            Err(e) => {
                out.failed += 1;
                out.error_directories.insert(WirePath::from_path(
                    relative.parent().unwrap_or(Path::new("")),
                )?);
                if out.issues.len() < limits.max_reported_issues {
                    out.issues.push(SyncIssue {
                        path: Some(relative.to_owned()),
                        operation: IssueOperation::Scan,
                        message: e.to_string(),
                    });
                }
                continue;
            }
        };
        // Prime the source-side cache used for deletion decisions. This does
        // not promise a snapshot against external changes to ignore files.
        if !out.filter.included(relative, info.is_dir())? {
            return Err(invalid("source ignore rules changed during traversal"));
        }
        let kind = if info.is_dir() {
            Kind::Directory
        } else if info.is_file() {
            Kind::File
        } else if info.file_type().is_symlink() && options.symlinks == SymlinkMode::Preserve {
            Kind::Symlink
        } else {
            Kind::Skipped
        };
        let mtime = FileTime::from_last_modification_time(&info);
        let wire = WirePath::from_path(relative)?;
        wire.path()?;
        let entry = Entry {
            path: wire,
            kind: kind.clone(),
            len: if kind == Kind::File { info.len() } else { 0 },
            mtime: mtime.unix_seconds(),
            nanos: mtime.nanoseconds(),
            mode: mode(&info),
            digest: if kind == Kind::File && options.comparison == Comparison::Checksum {
                Some(hash_file(path, cancel)?)
            } else {
                None
            },
            link: if kind == Kind::Symlink {
                Some(os_bytes(fs::read_link(path)?.as_os_str())?)
            } else {
                None
            },
        };
        bytes = bytes.saturating_add(
            serde_json::to_vec(&entry)
                .map_err(|e| invalid(e.to_string()))?
                .len()
                + 512,
        );
        if out.entries.len() >= limits.max_entries || bytes > limits.max_buffer_bytes / 8 {
            return Err(SyncError::LimitExceeded {
                resource: Resource::Entries,
            });
        }
        out.entries.push(entry);
    }
    // Process the current directory's files before descending into subdirectory
    // lists. Otherwise a nested scan error would incorrectly suppress actions
    // belonging to its parent's earlier file list (rsync incremental recursion).
    let directories: HashSet<_> = out
        .entries
        .iter()
        .filter(|e| e.kind == Kind::Directory)
        .map(|e| e.path.clone())
        .collect();
    out.entries.sort_by_cached_key(|e| {
        let mut prefix = WirePath(Vec::new());
        e.path
            .0
            .iter()
            .map(|name| {
                prefix.0.push(name.clone());
                (u8::from(directories.contains(&prefix)), name.clone())
            })
            .collect::<Vec<_>>()
    });
    Ok(out)
}
pub(crate) fn validate_manifest(entries: &[Entry]) -> Result<(), SyncError> {
    if entries
        .first()
        .is_none_or(|e| !e.path.0.is_empty() || e.kind != Kind::Directory)
    {
        return Err(invalid("manifest must begin with root directory"));
    }
    let mut seen = HashSet::new();
    let mut directories = HashSet::new();
    directories.insert(PathBuf::new());
    for (index, e) in entries.iter().enumerate() {
        let p = e.path.path()?;
        if (p.as_os_str().is_empty() && (index != 0 || e.kind != Kind::Directory))
            || !seen.insert(p.clone())
            || e.nanos >= 1_000_000_000
            || !directories.contains(p.parent().unwrap_or(Path::new("")))
        {
            return Err(invalid("invalid or unordered file tree"));
        }
        if e.kind == Kind::Directory {
            directories.insert(p);
        }
        if e.kind == Kind::Symlink
            && e.link
                .as_ref()
                .is_none_or(|s| s.is_empty() || s.contains(&0))
        {
            return Err(invalid("invalid symlink"));
        }
        if e.kind != Kind::Symlink && e.link.is_some() {
            return Err(invalid("unexpected symlink target"));
        }
    }
    Ok(())
}
pub(crate) fn metadata_changed(
    path: &Path,
    entry: &Entry,
    options: &SyncOptions,
) -> Result<bool, SyncError> {
    let info = match fs::symlink_metadata(path) {
        Ok(info) => info,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(e) => return Err(e.into()),
    };
    let time = FileTime::from_last_modification_time(&info);
    Ok((options.preserve_mtime
        && (time.unix_seconds() != entry.mtime || time.nanoseconds() != entry.nanos))
        || (options.preserve_permissions
            && entry.kind != Kind::Symlink
            && mode(&info) != entry.mode))
}
pub(crate) fn structural_changed(
    path: &Path,
    entry: &Entry,
    options: &SyncOptions,
) -> Result<bool, SyncError> {
    let info = match fs::symlink_metadata(path) {
        Ok(info) => info,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(e) => return Err(e.into()),
    };
    let structural = match entry.kind {
        Kind::Directory => !info.is_dir(),
        Kind::Symlink => {
            !info.file_type().is_symlink()
                || Some(os_bytes(fs::read_link(path)?.as_os_str())?) != entry.link
        }
        _ => false,
    };
    Ok(structural || metadata_changed(path, entry, options)?)
}

/// Refuse symlink ancestors within the transfer tree. This does not promise a
/// sandbox against concurrent hostile directory replacement by local processes.
pub(crate) fn target(root: &Path, relative: &Path) -> Result<PathBuf, SyncError> {
    let mut path = root.to_owned();
    let components: Vec<_> = relative.components().collect();
    for (i, c) in components.iter().enumerate() {
        if !matches!(c, Component::Normal(_)) {
            return Err(invalid("invalid target path"));
        }
        path.push(c.as_os_str());
        if i + 1 != components.len() {
            match fs::symlink_metadata(&path) {
                Ok(m) if m.is_dir() && !m.file_type().is_symlink() => {}
                Ok(_) => return Err(invalid("target ancestor is not a real directory")),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(path)
}
pub(crate) fn read_bounded(path: &Path, limit: usize) -> Result<Vec<u8>, SyncError> {
    let m = fs::symlink_metadata(path)?;
    if !m.is_file() || m.len() > limit as u64 {
        return Err(SyncError::LimitExceeded {
            resource: Resource::BufferMemory,
        });
    }
    let mut data = Vec::new();
    fs::File::open(path)?
        .take(limit as u64 + 1)
        .read_to_end(&mut data)?;
    if data.len() > limit {
        return Err(SyncError::LimitExceeded {
            resource: Resource::BufferMemory,
        });
    }
    Ok(data)
}
pub(crate) fn hash_file(path: &Path, cancel: &CancellationToken) -> Result<String, SyncError> {
    let mut file = fs::File::open(path)?;
    let mut hash = blake3::Hasher::new();
    let mut buffer = [0u8; 65536];
    loop {
        check_cancel(cancel)?;
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hash.finalize().to_hex().to_string())
}
#[cfg(unix)]
fn mode(m: &fs::Metadata) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    m.permissions().mode() & 0o7777
}
#[cfg(not(unix))]
fn mode(m: &fs::Metadata) -> u32 {
    if m.permissions().readonly() {
        0o444
    } else {
        0o666
    }
}

pub(crate) fn metadata(path: &Path, entry: &Entry, options: &SyncOptions) -> Result<(), SyncError> {
    if options.preserve_mtime {
        let time = FileTime::from_unix_time(entry.mtime, entry.nanos);
        if entry.kind == Kind::Symlink {
            filetime::set_symlink_file_times(path, time, time)?;
        } else {
            filetime::set_file_mtime(path, time)?;
        }
    }
    if options.preserve_permissions && entry.kind != Kind::Symlink {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(path, fs::Permissions::from_mode(entry.mode & 0o7777))?;
        }
        #[cfg(not(unix))]
        {
            return Err(invalid(
                "permission preservation unsupported on this platform",
            ));
        }
    }
    Ok(())
}
pub(crate) fn remove(path: &Path) -> Result<(), SyncError> {
    let m = fs::symlink_metadata(path)?;
    if m.is_dir() && !m.file_type().is_symlink() {
        fs::remove_dir(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}
pub(crate) fn ensure_dir(path: &Path, source_mode: u32) -> Result<Option<u32>, SyncError> {
    match fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() && !m.file_type().is_symlink() => return Ok(None),
        Ok(_) => remove(path)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    fs::create_dir(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let masked = fs::metadata(path)?.permissions().mode() & source_mode & 0o777;
        // Keep traversal/write access until descendants have been created.
        fs::set_permissions(path, fs::Permissions::from_mode(masked | 0o700))?;
        Ok(Some(masked))
    }
    #[cfg(not(unix))]
    {
        let _ = source_mode;
        Ok(None)
    }
}
pub(crate) fn set_mode(path: &Path, mode: u32) -> Result<(), SyncError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, mode);
    }
    Ok(())
}
pub(crate) fn stage_file(
    path: &Path,
    entry: &Entry,
    options: &SyncOptions,
    bytes: &[u8],
) -> Result<(), SyncError> {
    let mut temp = new_stage(path, entry)?;
    temp.write_all(bytes)?;
    temp.flush()?;
    commit_stage(temp, path, entry, options)
}
pub(crate) fn new_stage(path: &Path, entry: &Entry) -> Result<tempfile::NamedTempFile, SyncError> {
    let parent = path.parent().ok_or_else(|| invalid("missing parent"))?;
    let mut builder = tempfile::Builder::new();
    // New files inherit source ordinary mode masked by the receiver's umask;
    // tempfile's default 0600 would not match recursive rsync behavior.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        builder.permissions(fs::Permissions::from_mode(entry.mode & 0o777));
    }
    Ok(builder.tempfile_in(parent)?)
}
pub(crate) fn commit_stage(
    temp: tempfile::NamedTempFile,
    path: &Path,
    entry: &Entry,
    options: &SyncOptions,
) -> Result<(), SyncError> {
    if !options.preserve_permissions
        && let Ok(old) = fs::symlink_metadata(path)
        && old.is_file()
    {
        temp.as_file().set_permissions(old.permissions())?;
    }
    metadata(temp.path(), entry, options)?;
    if fs::symlink_metadata(path).is_ok_and(|m| m.is_dir()) {
        fs::remove_dir(path)?;
    }
    temp.persist(path).map_err(|e| SyncError::Io(e.error))?;
    Ok(())
}
pub(crate) fn symlink(path: &Path, entry: &Entry, options: &SyncOptions) -> Result<(), SyncError> {
    let link = bytes_os(entry.link.clone().ok_or_else(|| invalid("missing link"))?)?;
    if fs::read_link(path).is_ok_and(|old| old.as_os_str() == link) {
        return metadata(path, entry, options);
    }
    let tempdir = tempfile::tempdir_in(path.parent().ok_or_else(|| invalid("missing parent"))?)?;
    let staged = tempdir.path().join("link");
    #[cfg(unix)]
    std::os::unix::fs::symlink(link, &staged)?;
    #[cfg(not(unix))]
    {
        let _ = link;
        return Err(invalid("symlink preservation unsupported on this platform"));
    }
    metadata(&staged, entry, options)?;
    if fs::symlink_metadata(path).is_ok_and(|m| m.is_dir()) {
        fs::remove_dir(path)?;
    }
    fs::rename(staged, path)?;
    Ok(())
}
