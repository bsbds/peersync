use crate::{ConfigError, FilterMode, FilterRule, SyncError, fs::WirePath};
use globset::{GlobBuilder, GlobMatcher};
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IgnoreFile {
    pub directory: WirePath,
    pub contents: String,
}

pub(crate) struct Filter {
    globs: Vec<(GlobMatcher, bool, bool)>,
    git: Vec<(PathBuf, Gitignore)>,
}
impl Filter {
    pub fn new(mode: &FilterMode, ignores: &[IgnoreFile]) -> Result<Self, SyncError> {
        let mut result = Self {
            globs: Vec::new(),
            git: Vec::new(),
        };
        let mut pattern_bytes = 0usize;
        if let FilterMode::Glob(rules) = mode {
            if rules.len() > 1024 {
                return Err(ConfigError("too many filter rules (maximum 1024)".into()).into());
            }
            for rule in rules {
                let (pattern, included) = match rule {
                    FilterRule::Include(p) => (p, true),
                    FilterRule::Exclude(p) => (p, false),
                };
                pattern_bytes = pattern_bytes.saturating_add(pattern.len());
                if pattern.len() > 4096 || pattern_bytes > 65536 {
                    return Err(ConfigError("filter pattern budget exceeded".into()).into());
                }
                let glob = GlobBuilder::new(pattern)
                    .literal_separator(true)
                    .case_insensitive(false)
                    .backslash_escape(true)
                    .build()
                    .map_err(|e| ConfigError(e.to_string()))?
                    .compile_matcher();
                result.globs.push((glob, included, pattern.contains('/')));
            }
        }
        if matches!(mode, FilterMode::GitIgnore) {
            if ignores.len() > 1024 {
                return Err(ConfigError("too many .gitignore files (maximum 1024)".into()).into());
            }
            for file in ignores {
                pattern_bytes = pattern_bytes.saturating_add(file.contents.len());
                if pattern_bytes > 65536 {
                    return Err(ConfigError(".gitignore pattern budget exceeded".into()).into());
                }
                let directory = file.directory.path()?;
                let mut builder = GitignoreBuilder::new(&directory);
                for line in file.contents.lines() {
                    builder
                        .add_line(Some(directory.join(".gitignore")), line)
                        .map_err(|e| ConfigError(e.to_string()))?;
                }
                let matcher = builder.build().map_err(|e| ConfigError(e.to_string()))?;
                result.git.push((directory, matcher));
            }
            result
                .git
                .sort_by_key(|(p, _)| std::cmp::Reverse(p.components().count()));
        }
        Ok(result)
    }
    pub fn included(&self, path: &Path, is_dir: bool) -> bool {
        for (glob, include, full) in &self.globs {
            let candidate = if *full {
                path
            } else {
                Path::new(path.file_name().unwrap_or_default())
            };
            if glob.is_match(candidate) {
                return *include;
            }
        }
        for (dir, matcher) in &self.git {
            if path.starts_with(dir) && path != dir {
                let matched = matcher.matched_path_or_any_parents(path, is_dir);
                if matched.is_ignore() {
                    return false;
                }
                if matched.is_whitelist() {
                    return true;
                }
            }
        }
        true
    }
}
