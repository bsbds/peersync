//! Source-host rule evaluation. Never evaluate destination candidates using
//! the destination's HOME, Git configuration, or pre-existing ignore files.
use crate::{ConfigError, FilterOptions, Limits, Resource, SyncError};
use ignore::{
    IncrementalIgnore, WalkBuilder,
    overrides::{Override, OverrideBuilder},
};
use std::{
    collections::HashSet,
    path::{Component, Path, PathBuf},
};

fn overrides(root: &Path, options: &FilterOptions) -> Result<Override, SyncError> {
    if options.globs.len() > 1024
        || options.globs.iter().any(|pattern| pattern.len() > 4096)
        || options.globs.iter().map(String::len).sum::<usize>() > 65536
    {
        return Err(ConfigError("filter pattern budget exceeded".into()).into());
    }
    let mut overrides = OverrideBuilder::new(root);
    for pattern in &options.globs {
        overrides
            .add(pattern)
            .map_err(|e| ConfigError(e.to_string()))?;
    }
    overrides
        .build()
        .map_err(|e| ConfigError(e.to_string()).into())
}

pub(crate) fn validate(options: &FilterOptions) -> Result<(), SyncError> {
    overrides(Path::new("."), options).map(|_| ())
}

pub(crate) fn builder(root: &Path, options: &FilterOptions) -> Result<WalkBuilder, SyncError> {
    // WalkBuilder logs global-ignore load errors rather than returning them.
    // Synchronization must not turn an unreadable filter into deletion consent.
    if !options.no_ignore {
        let (_, error) = ignore::gitignore::GitignoreBuilder::new(root).build_global();
        if let Some(error) = error {
            return Err(ConfigError(format!("cannot load global ignore rules: {error}")).into());
        }
    }
    let mut walker = WalkBuilder::new(root);
    walker
        .standard_filters(!options.no_ignore)
        .hidden(!options.hidden)
        .follow_links(false)
        .overrides(overrides(root, options)?)
        .sort_by_file_path(|a, b| a.cmp(b));
    // WalkBuilder itself does not implicitly know ripgrep's custom filename.
    if !options.no_ignore {
        walker.add_custom_ignore_filename(".rgignore");
    }
    Ok(walker)
}

pub(crate) struct SourceFilter {
    root: PathBuf,
    matcher: IncrementalIgnore,
    contexts: HashSet<PathBuf>,
    remaining: usize,
}

impl SourceFilter {
    pub(crate) fn new(
        root: &Path,
        options: &FilterOptions,
        limits: &Limits,
    ) -> Result<Self, SyncError> {
        let remaining = (limits.max_buffer_bytes / 8)
            .checked_sub(root.as_os_str().len() + 512)
            .ok_or(SyncError::LimitExceeded {
                resource: Resource::BufferMemory,
            })?;
        Ok(Self {
            root: root.to_owned(),
            matcher: builder(root, options)?.build_matchers().remove(0),
            contexts: HashSet::new(),
            remaining,
        })
    }

    pub(crate) fn included(&mut self, path: &Path, is_dir: bool) -> Result<bool, SyncError> {
        if path.components().count() > 256
            || path
                .components()
                .any(|c| !matches!(c, Component::Normal(_)))
        {
            return Err(ConfigError("filter candidate must be root-relative".into()).into());
        }
        // A remote candidate must not cause the matcher to read ignore files
        // through a source symlink. Missing directories are legitimate: this
        // API also evaluates entries that exist only at the receiver.
        let mut prefix = self.root.clone();
        let components: Vec<_> = path.components().collect();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            prefix.push(component.as_os_str());
            match std::fs::symlink_metadata(&prefix) {
                Ok(info) if info.file_type().is_symlink() => return Ok(false),
                Ok(info) if !info.is_dir() => break,
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => return Err(error.into()),
            }
        }
        // A single deep candidate causes the upstream matcher to cache every
        // ancestor. Charge unique ancestor paths, not just the wire pathname,
        // before allowing that expansion. This is an estimate, not an RSS cap
        // for compilation of locally configured ignore files.
        let mut relative = PathBuf::new();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            relative.push(component.as_os_str());
            if !self.contexts.contains(&relative) {
                let cost = 512 + 3 * relative.as_os_str().len() + 2 * self.root.as_os_str().len();
                self.remaining =
                    self.remaining
                        .checked_sub(cost)
                        .ok_or(SyncError::LimitExceeded {
                            resource: Resource::BufferMemory,
                        })?;
                self.contexts.insert(relative.clone());
            }
        }
        let (matched, error) = self.matcher.matched_with_errors(path, is_dir);
        if let Some(error) = error {
            return Err(ConfigError(format!("cannot load source ignore rules: {error}")).into());
        }
        Ok(!matched.is_ignore())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_overrides_and_missing_candidates() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".ignore"), "ignored.tmp\nblocked/\n").unwrap();
        let mut filter =
            SourceFilter::new(root.path(), &FilterOptions::default(), &Limits::default()).unwrap();
        assert!(!filter.included(Path::new("ignored.tmp"), false).unwrap());
        assert!(
            !filter
                .included(Path::new("blocked/child.rs"), false)
                .unwrap()
        );
        assert!(!filter.included(Path::new(".hidden.rs"), false).unwrap());
        assert!(filter.included(Path::new("missing.rs"), false).unwrap());
        assert!(filter.included(Path::new("../escape"), false).is_err());
        let options = FilterOptions {
            globs: vec!["*.rs".into(), "!excluded.rs".into()],
            ..Default::default()
        };
        let mut filter = SourceFilter::new(root.path(), &options, &Limits::default()).unwrap();
        assert!(filter.included(Path::new(".hidden.rs"), false).unwrap());
        assert!(!filter.included(Path::new("excluded.rs"), false).unwrap());
        assert!(!filter.included(Path::new("other.txt"), false).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn candidate_cannot_read_rules_through_source_symlink() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), root.path().join("link")).unwrap();
        let mut filter =
            SourceFilter::new(root.path(), &FilterOptions::default(), &Limits::default()).unwrap();
        assert!(!filter.included(Path::new("link/child"), false).unwrap());
    }

    #[test]
    fn scanner_uses_new_defaults_and_retains_visitable_empty_directories() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join(".ignore"), "*.tmp\n").unwrap();
        for name in ["visible", ".hidden", "ignored.tmp"] {
            std::fs::write(root.path().join(name), b"data").unwrap();
        }
        std::fs::create_dir(root.path().join("empty")).unwrap();
        let mut manifest = crate::fs::scan(
            root.path(),
            &crate::SyncOptions::default(),
            &crate::Limits::default(),
            &tokio_util::sync::CancellationToken::new(),
        )
        .unwrap();
        let paths: std::collections::HashSet<_> = manifest
            .entries
            .iter()
            .map(|entry| entry.path.path().unwrap())
            .collect();
        assert_eq!(paths, ["", "visible", "empty"].map(PathBuf::from).into());
        assert!(
            !manifest
                .filter
                .included(Path::new("future.tmp"), false)
                .unwrap()
        );
    }

    #[test]
    fn deep_missing_candidates_cannot_expand_cache_past_budget() {
        let root = tempfile::tempdir().unwrap();
        let limits = Limits {
            max_buffer_bytes: 8192,
            ..Limits::default()
        };
        let mut filter =
            SourceFilter::new(root.path(), &FilterOptions::default(), &limits).unwrap();
        assert!(matches!(
            filter.included(Path::new("a/b/c/file"), false),
            Err(SyncError::LimitExceeded {
                resource: Resource::BufferMemory
            })
        ));
    }
}
