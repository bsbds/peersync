use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;

/// Options proposed by the sender. Filtering defaults follow ripgrep;
/// transfer and metadata options remain a limited recursive sync profile.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SyncOptions {
    pub comparison: Comparison,
    pub transfer: TransferMode,
    pub delete: bool,
    pub dry_run: bool,
    pub preserve_mtime: bool,
    pub preserve_permissions: bool,
    pub symlinks: SymlinkMode,
    pub filter: FilterOptions,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Comparison {
    #[default]
    SizeAndMtime,
    Checksum,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum TransferMode {
    #[default]
    Delta,
    WholeFile,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum SymlinkMode {
    #[default]
    Skip,
    Preserve,
}

/// Ripgrep-style filtering. Explicit globs override ignore files, with the
/// last matching glob winning. Defaults hide hidden entries and load ignore
/// sources from the source host, including parent and Git configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct FilterOptions {
    pub globs: Vec<String>,
    /// Like `rg --hidden`: ignore files can still exclude hidden entries.
    pub hidden: bool,
    /// Like `rg --no-ignore`: explicit globs and hidden filtering still apply.
    pub no_ignore: bool,
}

/// Conservative initial budgets, not performance claims. One task at a time per
/// Syncer shares the budget. Concurrent use of that instance returns a resource
/// error rather than waiting. Independent Syncers have independent budgets.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Limits {
    pub max_control_frame_bytes: usize,
    pub max_data_frame_bytes: usize,
    pub max_entries: usize,
    pub max_buffer_bytes: usize,
    pub max_reported_issues: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            max_control_frame_bytes: 256 * 1024,
            max_data_frame_bytes: 64 * 1024,
            max_entries: 100_000,
            max_buffer_bytes: 128 * 1024 * 1024,
            max_reported_issues: 100,
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.max_control_frame_bytes < 1024
            || self.max_data_frame_bytes == 0
            || self.max_control_frame_bytes > u32::MAX as usize
            || self.max_data_frame_bytes > u32::MAX as usize
            || self.max_entries == 0
            || self.max_buffer_bytes < 1024 * 1024
            || self.max_control_frame_bytes > self.max_buffer_bytes / 8
            || self.max_data_frame_bytes > self.max_buffer_bytes / 8
        {
            return Err(ConfigError(
                "invalid or inconsistent resource limits".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub(crate) String);
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SyncError {
    #[error("invalid options: {0}")]
    InvalidOptions(#[from] ConfigError),
    #[error("incompatible protocol")]
    IncompatibleProtocol,
    #[error("incompatible delta format")]
    IncompatibleDeltaFormat,
    #[error("sender options do not match the approved operation")]
    OptionsMismatch,
    #[error("peer rejected operation: {reason}")]
    Rejected { reason: String },
    #[error("protocol error: {message}")]
    Protocol { message: String },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("resource limit exceeded: {resource:?}")]
    LimitExceeded { resource: Resource },
    #[error("verification failed: {path:?}")]
    VerificationFailed { path: PathBuf },
    #[error("one or more items failed")]
    PartialFailure,
    #[error("operation cancelled")]
    Cancelled,
    #[error("delta engine: {0}")]
    Delta(#[from] crate::delta::DeltaError),
}
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Resource {
    ControlFrame,
    DataFrame,
    Entries,
    BufferMemory,
    Issues,
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SyncReport {
    pub dry_run: bool,
    pub scanned_entries: u64,
    pub completed_items: u64,
    pub skipped_items: u64,
    pub failed_items: u64,
    pub planned_changes: u64,
    pub committed_changes: u64,
    pub sent_bytes: u64,
    pub received_bytes: u64,
    pub duration: Duration,
    pub peer_completion_confirmed: bool,
    pub effects_may_be_incomplete_or_unknown: bool,
    pub issues: Vec<SyncIssue>,
    pub omitted_issues: u64,
}
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct SyncFailure {
    #[source]
    pub error: SyncError,
    pub report: Box<SyncReport>,
}
pub type SyncResult = Result<SyncReport, SyncFailure>;

#[derive(Debug, Clone)]
pub struct SyncIssue {
    pub path: Option<PathBuf>,
    pub operation: IssueOperation,
    pub message: String,
}
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum IssueOperation {
    Scan,
    Read,
    Write,
    Verify,
    Metadata,
    Delete,
}
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum SyncEvent {
    PhaseChanged(Phase),
    Scanned {
        entries: u64,
    },
    ItemStarted {
        path: PathBuf,
        action: ItemAction,
    },
    Progress {
        sent_bytes: u64,
        received_bytes: u64,
    },
    ItemFinished {
        path: PathBuf,
        action: ItemAction,
    },
    Retrying {
        path: PathBuf,
        attempt: u32,
    },
    Issue(SyncIssue),
}
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum Phase {
    Handshake,
    Scan,
    Transfer,
    Delete,
    Finish,
}
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum ItemAction {
    CreateFile,
    UpdateFile,
    CreateDirectory,
    CreateSymlink,
    UpdateMetadata,
    Delete,
    Skip,
}

/// Per-operation cancellation and optional fast, nonblocking observer.
/// Cancelling invalidates the protocol stream. Await the operation after cancel;
/// dropping its future cannot wait for cleanup of blocking work.
pub struct SyncContext<'a> {
    pub(crate) cancel: CancellationToken,
    observer: Option<&'a mut (dyn FnMut(&SyncEvent) + Send)>,
    last_progress: Option<std::time::Instant>,
}
impl<'a> SyncContext<'a> {
    pub fn new(cancel: CancellationToken) -> Self {
        Self {
            cancel,
            observer: None,
            last_progress: None,
        }
    }
    #[must_use]
    pub fn with_progress(mut self, observer: &'a mut (dyn FnMut(&SyncEvent) + Send)) -> Self {
        self.observer = Some(observer);
        self
    }
    pub(crate) fn emit(&mut self, event: SyncEvent) {
        if self.observer.is_none() {
            return;
        }
        if matches!(event, SyncEvent::Progress { .. }) {
            let now = std::time::Instant::now();
            if self
                .last_progress
                .is_some_and(|last| now.duration_since(last) < Duration::from_millis(100))
            {
                return;
            }
            self.last_progress = Some(now);
        }
        if let Some(observer) = &mut self.observer {
            observer(&event);
        }
    }
}
