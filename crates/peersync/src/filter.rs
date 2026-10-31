use crate::fs::WirePath;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

pub(crate) mod ripgrep;

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Candidate {
    pub path: WirePath,
    pub is_dir: bool,
}

/// Only explicitly approved path/type pairs may be removed. A target entry
/// created or replaced after planning is conservatively protected.
#[derive(Default)]
pub(crate) struct Filter {
    allowed: HashSet<(PathBuf, bool)>,
}
impl Filter {
    pub(crate) fn allow(&mut self, path: PathBuf, is_dir: bool) {
        self.allowed.insert((path, is_dir));
    }
    pub(crate) fn included(&self, path: &Path, is_dir: bool) -> bool {
        self.allowed.contains(&(path.to_owned(), is_dir))
    }
}
