use crate::{
    delta::{DeltaContext, DeltaEngine, FastRsync, signature_options},
    filter::Filter,
    fs::{Entry, Kind, WirePath},
    protocol::{Message, VERSION, Wire, invalid},
    *,
};
use std::{
    collections::{HashMap, HashSet},
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Reusable synchronization engine. One task may use its memory budget at a
/// time. Concurrent use fails fast rather than waiting across network I/O,
/// which could deadlock when both protocol roles share an instance.
pub struct Syncer {
    engine: Arc<dyn DeltaEngine>,
    limits: Limits,
    slot: tokio::sync::Semaphore,
}
impl Default for Syncer {
    fn default() -> Self {
        Self::new()
    }
}
impl Syncer {
    pub fn new() -> Self {
        Self::with_engine(FastRsync)
    }
    pub fn with_engine<E: DeltaEngine + 'static>(engine: E) -> Self {
        Self {
            engine: Arc::new(engine),
            limits: Limits::default(),
            slot: tokio::sync::Semaphore::new(1),
        }
    }
    pub fn with_limits(mut self, limits: Limits) -> Result<Self, ConfigError> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    /// Send directory contents over borrowed, exclusive read/write streams.
    ///
    /// # Errors
    /// Returns a failure with partial statistics on I/O, protocol, resource or
    /// file errors. Effects may already exist at the peer; do not auto-replay.
    ///
    /// # Cancellation
    /// Cancel the context token and continue awaiting for cooperative cleanup.
    /// Dropping this future cannot await blocking work. Discard streams on failure.
    pub async fn send<R, W>(
        &self,
        reader: &mut R,
        writer: &mut W,
        source: &Path,
        options: &SyncOptions,
        context: &mut SyncContext<'_>,
    ) -> SyncResult
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let start = Instant::now();
        let mut report = SyncReport {
            dry_run: options.dry_run,
            ..Default::default()
        };
        let mut wire = Wire {
            reader,
            writer,
            limits: &self.limits,
            cancel: context.cancel.clone(),
            sent: 0,
            received: 0,
        };
        let result = async {
            fs::check_cancel(&context.cancel)?;
            let _permit = self
                .slot
                .try_acquire()
                .map_err(|_| SyncError::LimitExceeded {
                    resource: Resource::BufferMemory,
                })?;
            self.send_inner(&mut wire, source, options, context, &mut report)
                .await
        }
        .await;
        self.finish(result, &mut wire, report, start, context).await
    }

    /// Receive into the locally selected directory. The caller must authorize
    /// the peer and operation before invoking this method; there is no extra
    /// deletion-permission layer. Missing destination roots may be created.
    ///
    /// # Errors
    /// Errors include a partial report, not a rollback guarantee.
    /// # Cancellation
    /// See [`Self::send`]. Cancellation invalidates the protocol stream.
    pub async fn receive<R, W>(
        &self,
        reader: &mut R,
        writer: &mut W,
        destination: &Path,
        context: &mut SyncContext<'_>,
    ) -> SyncResult
    where
        R: AsyncRead + Unpin + Send,
        W: AsyncWrite + Unpin + Send,
    {
        let start = Instant::now();
        let report = SyncReport::default();
        let mut wire = Wire {
            reader,
            writer,
            limits: &self.limits,
            cancel: context.cancel.clone(),
            sent: 0,
            received: 0,
        };
        let mut report = report;
        let result = async {
            fs::check_cancel(&context.cancel)?;
            let _permit = self
                .slot
                .try_acquire()
                .map_err(|_| SyncError::LimitExceeded {
                    resource: Resource::BufferMemory,
                })?;
            self.receive_inner(&mut wire, destination, context, &mut report)
                .await
        }
        .await;
        self.finish(result, &mut wire, report, start, context).await
    }

    async fn finish<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send>(
        &self,
        result: Result<(), SyncError>,
        wire: &mut Wire<'_, R, W>,
        mut report: SyncReport,
        start: Instant,
        context: &mut SyncContext<'_>,
    ) -> SyncResult {
        if let Err(error) = &result {
            tracing::warn!(error = %error, "synchronization failed");
            // Best effort terminal notice only; never let an unresponsive peer
            // turn cleanup into an unbounded wait. Partial frames remain invalid.
            if !matches!(error, SyncError::Cancelled | SyncError::PartialFailure) {
                let _ = tokio::time::timeout(
                    std::time::Duration::from_millis(100),
                    wire.send(&Message::Failed {
                        message: error.to_string().chars().take(512).collect(),
                    }),
                )
                .await;
            }
            report.effects_may_be_incomplete_or_unknown = true;
        }
        report.sent_bytes = wire.sent;
        report.received_bytes = wire.received;
        report.duration = start.elapsed();
        context.emit(SyncEvent::Progress {
            sent_bytes: wire.sent,
            received_bytes: wire.received,
        });
        match result {
            Ok(()) => Ok(report),
            Err(error) => Err(SyncFailure {
                error,
                report: Box::new(report),
            }),
        }
    }

    async fn send_inner<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send>(
        &self,
        wire: &mut Wire<'_, R, W>,
        source: &Path,
        options: &SyncOptions,
        context: &mut SyncContext<'_>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        context.emit(SyncEvent::PhaseChanged(Phase::Handshake));
        Filter::new(&options.filter, &[])?;
        let format = self.engine.format();
        wire.send(&Message::Hello {
            version: VERSION,
            signature: format.signature_id.into(),
            delta: format.delta_id.into(),
            format_version: format.version,
            options: options.clone(),
        })
        .await?;
        match wire.receive().await? {
            Message::Ready => {}
            Message::Failed { message } => return Err(SyncError::Rejected { reason: message }),
            _ => return Err(invalid("expected ready")),
        }
        let root = fs::absolute(source)?;
        context.emit(SyncEvent::PhaseChanged(Phase::Scan));
        let root_clone = root.clone();
        let opts = options.clone();
        let limits = self.limits.clone();
        let cancel = context.cancel.clone();
        let manifest = blocking(move || fs::scan(&root_clone, &opts, &limits, &cancel)).await?;
        report.scanned_entries = manifest.entries.len() as u64;
        context.emit(SyncEvent::Scanned {
            entries: report.scanned_entries,
        });
        for ignore in manifest.ignores {
            wire.send(&Message::IgnoreFile(ignore)).await?;
        }
        for entry in &manifest.entries {
            wire.send(&Message::Entry(entry.clone())).await?;
        }
        // Associate errors with the directory whose file list failed. Deletion
        // already performed in an ancestor is not retrospectively suppressed.
        for directory in manifest.error_directories {
            wire.send(&Message::ScanIssue {
                path: Some(directory),
                message: "source directory scan failed".into(),
            })
            .await?;
        }
        for issue in manifest.issues {
            self.issue(report, context, issue);
        }
        report.failed_items = manifest.failed;
        wire.send(&Message::ManifestEnd).await?;
        context.emit(SyncEvent::PhaseChanged(Phase::Transfer));
        let mut requests: HashMap<usize, u8> = HashMap::new();
        loop {
            match wire.receive().await? {
                Message::Request {
                    index,
                    hash_bytes,
                    signature_len,
                } => {
                    if options.dry_run {
                        return Err(invalid("data request in dry-run"));
                    }
                    let entry = manifest
                        .entries
                        .get(index)
                        .filter(|e| e.kind == Kind::File)
                        .ok_or_else(|| invalid("invalid file index"))?;
                    let attempts = requests.entry(index).or_default();
                    *attempts += 1;
                    if *attempts > 2
                        || !(1..=16).contains(&hash_bytes)
                        || (*attempts == 2 && hash_bytes != 16)
                    {
                        return Err(invalid("invalid retry parameters"));
                    }
                    let signature = wire
                        .receive_data(signature_len, self.limits.max_buffer_bytes / 16)
                        .await?;
                    let relative = entry.path.path()?;
                    let path = target_path(&root, &relative).await?;
                    if signature_len == 0 {
                        let p = path.clone();
                        let len = entry.len;
                        let file = blocking(move || {
                            if !std::fs::symlink_metadata(&p)?.is_file() {
                                return Err(invalid("source type changed"));
                            }
                            let file = std::fs::File::open(&p)?;
                            if file.metadata()?.len() != len {
                                return Err(invalid("source length changed"));
                            }
                            Ok(file)
                        })
                        .await;
                        match file {
                            Ok(file) => {
                                wire.send(&Message::Content {
                                    bytes: len,
                                    file_len: len,
                                    digest: String::new(),
                                    delta: false,
                                })
                                .await?;
                                let mut file = tokio::fs::File::from_std(file);
                                let mut remaining = len;
                                let mut hash = blake3::Hasher::new();
                                let mut chunk = vec![0u8; self.limits.max_data_frame_bytes];
                                while remaining > 0 {
                                    let size = remaining.min(chunk.len() as u64) as usize;
                                    let n = tokio::select! { biased; ()=context.cancel.cancelled()=>return Err(SyncError::Cancelled), n=file.read(&mut chunk[..size])=>n? };
                                    if n == 0 {
                                        return Err(invalid("source truncated during transfer"));
                                    }
                                    hash.update(&chunk[..n]);
                                    wire.send_data(&chunk[..n]).await?;
                                    context.emit(SyncEvent::Progress {
                                        sent_bytes: wire.sent,
                                        received_bytes: wire.received,
                                    });
                                    remaining -= n as u64;
                                }
                                let mut extra = [0u8; 1];
                                if file.read(&mut extra).await? != 0 {
                                    return Err(invalid("source grew during transfer"));
                                }
                                wire.send(&Message::ContentEnd {
                                    digest: hash.finalize().to_hex().to_string(),
                                })
                                .await?;
                            }
                            Err(error) => {
                                self.issue(
                                    report,
                                    context,
                                    SyncIssue {
                                        path: Some(relative),
                                        operation: IssueOperation::Read,
                                        message: error.to_string(),
                                    },
                                );
                                wire.send(&Message::Failed {
                                    message: error.to_string().chars().take(512).collect(),
                                })
                                .await?;
                            }
                        }
                        continue;
                    }
                    let limits = self.limits.clone();
                    let engine = self.engine.clone();
                    let cancel = context.cancel.clone();
                    let use_delta = signature_len > 0 && options.transfer == TransferMode::Delta;
                    let expected_len = entry.len;
                    let prepared = blocking(move || {
                        fs::check_cancel(&cancel)?;
                        let source = fs::read_bounded(&path, limits.max_buffer_bytes / 16)?;
                        if source.len() as u64 != expected_len {
                            return Err(invalid("source size changed during sync"));
                        }
                        let digest = blake3::hash(&source).to_hex().to_string();
                        if !use_delta {
                            return Ok((source, digest));
                        }
                        // Signature parsing belongs to the negotiated engine,
                        // not the transport. FastRsync validates before indexing.
                        let mut output = Vec::new();
                        let ctx = DeltaContext::new(
                            &cancel,
                            limits.max_buffer_bytes / 2,
                            (limits.max_buffer_bytes / 8) as u64,
                        );
                        engine.delta(
                            &mut &signature[..],
                            &mut Cursor::new(&source),
                            source.len() as u64,
                            &mut output,
                            &ctx,
                        )?;
                        fs::check_cancel(&cancel)?;
                        Ok((output, digest))
                    })
                    .await;
                    match prepared {
                        Ok((data, digest)) => {
                            wire.send(&Message::Content {
                                bytes: data.len() as u64,
                                file_len: entry.len,
                                digest,
                                delta: use_delta,
                            })
                            .await?;
                            wire.send_data(&data).await?;
                            context.emit(SyncEvent::Progress {
                                sent_bytes: wire.sent,
                                received_bytes: wire.received,
                            });
                        }
                        Err(SyncError::Cancelled) => return Err(SyncError::Cancelled),
                        Err(error) => {
                            self.issue(
                                report,
                                context,
                                SyncIssue {
                                    path: Some(relative),
                                    operation: IssueOperation::Read,
                                    message: error.to_string(),
                                },
                            );
                            wire.send(&Message::Failed {
                                message: error.to_string().chars().take(512).collect(),
                            })
                            .await?;
                        }
                    }
                }
                Message::ItemResult { committed } => {
                    report.completed_items += 1;
                    if committed {
                        report.committed_changes += 1;
                    }
                }
                Message::Done {
                    failed,
                    committed,
                    planned,
                    skipped,
                } => {
                    report.failed_items = report.failed_items.max(failed);
                    report.committed_changes = committed;
                    report.planned_changes = planned;
                    report.skipped_items = skipped;
                    report.peer_completion_confirmed = true;
                    wire.send(&Message::Finish).await?;
                    context.emit(SyncEvent::PhaseChanged(Phase::Finish));
                    return if report.failed_items == 0 {
                        Ok(())
                    } else {
                        Err(SyncError::PartialFailure)
                    };
                }
                Message::Failed { message } => return Err(SyncError::Rejected { reason: message }),
                _ => return Err(invalid("unexpected sender state")),
            }
        }
    }

    async fn receive_inner<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send>(
        &self,
        wire: &mut Wire<'_, R, W>,
        destination: &Path,
        context: &mut SyncContext<'_>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        context.emit(SyncEvent::PhaseChanged(Phase::Handshake));
        let (options, signature, delta, format_version) = match wire.receive().await? {
            Message::Hello {
                version,
                signature,
                delta,
                format_version,
                options,
            } if version == VERSION => (options, signature, delta, format_version),
            _ => return Err(SyncError::IncompatibleProtocol),
        };
        let format = self.engine.format();
        if options.transfer == TransferMode::Delta
            && (format.signature_id != signature
                || format.delta_id != delta
                || format.version != format_version)
        {
            return Err(SyncError::IncompatibleDeltaFormat);
        }
        Filter::new(&options.filter, &[])?;
        #[cfg(not(unix))]
        if options.preserve_permissions || options.symlinks == SymlinkMode::Preserve {
            return Err(invalid("requested metadata unsupported on this platform"));
        }
        report.dry_run = options.dry_run;
        wire.send(&Message::Ready).await?;
        let mut entries = Vec::new();
        let mut ignores = Vec::new();
        let mut manifest_bytes = 0;
        let mut pending_scan_errors = HashMap::new();
        loop {
            let message = wire.receive().await?;
            manifest_bytes += serde_json::to_vec(&message)
                .map_err(|e| invalid(e.to_string()))?
                .len()
                + 512;
            if manifest_bytes > self.limits.max_buffer_bytes / 8
                || entries.len() + ignores.len() > self.limits.max_entries
            {
                return Err(SyncError::LimitExceeded {
                    resource: Resource::Entries,
                });
            }
            match message {
                Message::Entry(e) => {
                    entries.push(e);
                }
                Message::IgnoreFile(f) if matches!(options.filter, FilterMode::GitIgnore) => {
                    f.directory.path()?;
                    ignores.push(f);
                }
                Message::ScanIssue { path, message } => {
                    pending_scan_errors.insert(
                        path.map(|p| p.path()).transpose()?.unwrap_or_default(),
                        message,
                    );
                }
                Message::ManifestEnd => break,
                Message::Failed { message } => return Err(SyncError::Rejected { reason: message }),
                _ => return Err(invalid("unexpected manifest message")),
            }
        }
        fs::validate_manifest(&entries)?;
        for path in pending_scan_errors.keys() {
            if !entries
                .iter()
                .any(|e| e.kind == Kind::Directory && e.path.path().is_ok_and(|p| &p == path))
            {
                return Err(invalid("scan error names an unlisted directory"));
            }
        }
        let filter = Arc::new(Filter::new(&options.filter, &ignores)?);
        let root = fs::absolute(destination)?;
        report.scanned_entries = entries.len() as u64;
        context.emit(SyncEvent::Scanned {
            entries: report.scanned_entries,
        });
        if !options.dry_run {
            let root = root.clone();
            blocking(move || {
                std::fs::create_dir_all(&root)?;
                Ok(())
            })
            .await?;
        }
        let expected: Arc<HashSet<PathBuf>> = Arc::new(
            entries
                .iter()
                .map(|e| e.path.path())
                .collect::<Result<_, _>>()?,
        );
        let mut created_directory_modes = HashMap::new();
        // delete-during: clean immediate children of each visited directory
        // before transferring its children. Apply source errors at the affected
        // directory's visit, not retroactively to already processed ancestors.
        for (index, entry) in entries.iter().enumerate() {
            fs::check_cancel(&context.cancel)?;
            let relative = entry.path.path()?;
            if let Some(message) = pending_scan_errors.remove(&relative) {
                self.issue(
                    report,
                    context,
                    SyncIssue {
                        path: Some(relative.clone()),
                        operation: IssueOperation::Scan,
                        message,
                    },
                );
            }
            if entry.kind == Kind::Skipped {
                report.skipped_items += 1;
                context.emit(SyncEvent::ItemFinished {
                    path: relative,
                    action: ItemAction::Skip,
                });
                continue;
            }
            let checked_path = target_path(&root, &relative).await;
            if options.dry_run && matches!(&checked_path, Err(SyncError::Protocol { .. })) {
                // A preceding directory replacement is only planned. Do not
                // inspect its old symlink target while simulating descendants.
                report.planned_changes += 1;
                report.completed_items += 1;
                continue;
            }
            let path = checked_path?;
            let p = path.clone();
            let target_is_dir = blocking(move || match std::fs::symlink_metadata(p) {
                Ok(m) => Ok(Some(m.is_dir())),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e.into()),
            })
            .await?;
            if entry.kind != Kind::Directory
                && options.delete
                && report.failed_items == 0
                && target_is_dir == Some(true)
            {
                // Ordinary --delete allows making way for a file/link by
                // removing an old directory's contents; filters still protect
                // excluded descendants and prevent removing a nonempty parent.
                self.delete_directory(
                    wire, &root, &relative, &expected, &filter, &options, context, report,
                )
                .await?;
            }
            let action = match entry.kind {
                Kind::Directory => ItemAction::CreateDirectory,
                Kind::Symlink => ItemAction::CreateSymlink,
                Kind::File => ItemAction::UpdateFile,
                Kind::Skipped => ItemAction::Skip,
            };
            let structural_changed = if entry.kind != Kind::File {
                let p = path.clone();
                let e = entry.clone();
                let o = options.clone();
                blocking(move || fs::structural_changed(&p, &e, &o)).await?
            } else {
                false
            };
            context.emit(SyncEvent::ItemStarted {
                path: relative.clone(),
                action,
            });
            let result = match entry.kind {
                Kind::Directory => {
                    let p = path.clone();
                    let dry = options.dry_run;
                    let is_root = relative.as_os_str().is_empty();
                    let mode = entry.mode;
                    match blocking(move || {
                        if !dry && !is_root {
                            fs::ensure_dir(&p, mode)
                        } else {
                            Ok(None)
                        }
                    })
                    .await
                    {
                        Ok(Some(mode)) => {
                            created_directory_modes.insert(relative.clone(), mode);
                            Ok(())
                        }
                        Ok(None) => Ok(()),
                        Err(e) => Err(e),
                    }
                }
                Kind::Symlink => {
                    if options.symlinks != SymlinkMode::Preserve {
                        return Err(invalid("unexpected symlink entry"));
                    }
                    let p = path.clone();
                    let e = entry.clone();
                    let o = options.clone();
                    blocking(move || {
                        if !o.dry_run {
                            fs::symlink(&p, &e, &o)?;
                        }
                        Ok(())
                    })
                    .await
                }
                Kind::File => {
                    self.receive_file(wire, index, entry, &path, &options, context, report)
                        .await
                }
                Kind::Skipped => unreachable!("handled before file operations"),
            };
            match result {
                Ok(()) => {
                    if entry.kind != Kind::File && structural_changed {
                        report.planned_changes += 1;
                        if !options.dry_run {
                            report.committed_changes += 1;
                        }
                    }
                    report.completed_items += 1;
                    context.emit(SyncEvent::ItemFinished {
                        path: relative.clone(),
                        action,
                    });
                    if entry.kind == Kind::Directory
                        && !(options.dry_run && target_is_dir == Some(false))
                    {
                        self.delete_directory(
                            wire, &root, &relative, &expected, &filter, &options, context, report,
                        )
                        .await?;
                    }
                }
                Err(SyncError::Cancelled) => return Err(SyncError::Cancelled),
                Err(error @ (SyncError::Protocol { .. } | SyncError::Io(_))) => {
                    // Transport and filesystem I/O share the public category;
                    // fail the task rather than continue an uncertain framing state.
                    self.issue(
                        report,
                        context,
                        SyncIssue {
                            path: Some(relative),
                            operation: IssueOperation::Write,
                            message: error.to_string(),
                        },
                    );
                    return Err(error);
                }
                Err(SyncError::PartialFailure) => {} // already recorded by the file operation
                Err(error) => {
                    self.issue(
                        report,
                        context,
                        SyncIssue {
                            path: Some(relative),
                            operation: IssueOperation::Write,
                            message: error.to_string(),
                        },
                    );
                }
            }
        }
        if !pending_scan_errors.is_empty() {
            return Err(invalid("scan error names an unlisted directory"));
        }
        // Directory mtimes/modes must be applied after descendants, otherwise
        // creating children changes mtimes and restrictive modes block writes.
        if !options.dry_run {
            for entry in entries.iter().rev().filter(|e| e.kind == Kind::Directory) {
                let relative = entry.path.path()?;
                let path = target_path(&root, &relative).await?;
                let e = entry.clone();
                let o = options.clone();
                let mode = created_directory_modes.get(&relative).copied();
                if let Err(error) = blocking(move || {
                    if !o.preserve_permissions
                        && let Some(mode) = mode
                    {
                        fs::set_mode(&path, mode)?;
                    }
                    fs::metadata(&path, &e, &o)
                })
                .await
                {
                    self.issue(
                        report,
                        context,
                        SyncIssue {
                            path: Some(entry.path.path()?),
                            operation: IssueOperation::Metadata,
                            message: error.to_string(),
                        },
                    );
                }
            }
        }
        wire.send(&Message::Done {
            failed: report.failed_items,
            committed: report.committed_changes,
            planned: report.planned_changes,
            skipped: report.skipped_items,
        })
        .await?;
        if !matches!(wire.receive().await?, Message::Finish) {
            return Err(invalid("expected finish acknowledgment"));
        }
        report.peer_completion_confirmed = true;
        context.emit(SyncEvent::PhaseChanged(Phase::Finish));
        if report.failed_items == 0 {
            Ok(())
        } else {
            Err(SyncError::PartialFailure)
        }
    }

    // Explicit operation inputs keep protocol state out of public/global types.
    #[allow(clippy::too_many_arguments)]
    async fn receive_file<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send>(
        &self,
        wire: &mut Wire<'_, R, W>,
        index: usize,
        entry: &Entry,
        path: &Path,
        options: &SyncOptions,
        context: &mut SyncContext<'_>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        let p = path.to_owned();
        let e = entry.clone();
        let o = options.clone();
        let cancel = context.cancel.clone();
        let skip = blocking(move || {
            let old = match std::fs::symlink_metadata(&p) {
                Ok(m) => m,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
                Err(err) => return Err(err.into()),
            };
            if !old.is_file() || old.len() != e.len {
                return Ok(false);
            }
            match o.comparison {
                Comparison::SizeAndMtime => Ok(filetime::FileTime::from_last_modification_time(
                    &old,
                )
                .unix_seconds()
                    == e.mtime),
                Comparison::Checksum => Ok(e
                    .digest
                    .as_ref()
                    .is_some_and(|h| fs::hash_file(&p, &cancel).is_ok_and(|v| &v == h))),
            }
        })
        .await?;
        if skip {
            report.skipped_items += 1;
            let p = path.to_owned();
            let e = entry.clone();
            let o = options.clone();
            let changed = blocking(move || fs::metadata_changed(&p, &e, &o)).await?;
            if changed {
                report.planned_changes += 1;
            }
            if !options.dry_run {
                let p = path.to_owned();
                let e = entry.clone();
                let o = options.clone();
                blocking(move || fs::metadata(&p, &e, &o)).await?;
            }
            if changed && !options.dry_run {
                report.committed_changes += 1;
            }
            return Ok(());
        }
        report.planned_changes += 1;
        if options.dry_run {
            return Ok(());
        }
        let p = path.to_owned();
        let budget = self.limits.max_buffer_bytes / 16;
        let can_delta = options.transfer == TransferMode::Delta && entry.len <= budget as u64;
        let basis = blocking(move || {
            if can_delta
                && std::fs::symlink_metadata(&p)
                    .is_ok_and(|m| m.is_file() && m.len() <= budget as u64)
            {
                fs::read_bounded(&p, budget)
            } else {
                Ok(Vec::new())
            }
        })
        .await?;
        let basis = Arc::new(basis);
        for attempt in 0..2 {
            let engine = self.engine.clone();
            let base = basis.clone();
            let cancel = context.cancel.clone();
            let budget = self.limits.max_buffer_bytes / 2;
            let params = signature_options(basis.len() as u64, attempt == 1);
            let use_delta = !basis.is_empty() && options.transfer == TransferMode::Delta;
            let signature = if use_delta {
                blocking(move || {
                    let mut out = Vec::new();
                    let ctx = DeltaContext::new(&cancel, budget, (budget / 8) as u64);
                    engine.signature(
                        &mut Cursor::new(base.as_slice()),
                        base.len() as u64,
                        &params,
                        &mut out,
                        &ctx,
                    )?;
                    Ok(out)
                })
                .await?
            } else {
                Vec::new()
            };
            wire.send(&Message::Request {
                index,
                hash_bytes: params.hash_bytes,
                signature_len: signature.len() as u64,
            })
            .await?;
            wire.send_data(&signature).await?;
            let (bytes, file_len, digest, delta) = match wire.receive().await? {
                Message::Content {
                    bytes,
                    file_len,
                    digest,
                    delta,
                } => (bytes, file_len, digest, delta),
                Message::Failed { message } => {
                    self.issue(
                        report,
                        context,
                        SyncIssue {
                            path: Some(entry.path.path()?),
                            operation: IssueOperation::Read,
                            message,
                        },
                    );
                    return Err(SyncError::PartialFailure);
                }
                _ => return Err(invalid("expected content")),
            };
            if file_len != entry.len
                || delta != use_delta
                || (delta && digest.len() != 64)
                || (!delta && (!digest.is_empty() || bytes != file_len))
            {
                return Err(invalid("invalid content metadata"));
            }
            if !delta {
                let p = path.to_owned();
                let e = entry.clone();
                let (temp, file) = blocking(move || {
                    let temp = fs::new_stage(&p, &e)?;
                    let file = temp.reopen()?;
                    Ok((temp, file))
                })
                .await?;
                let mut file = tokio::fs::File::from_std(file);
                let mut remaining = file_len;
                let mut hash = blake3::Hasher::new();
                while remaining > 0 {
                    let chunk = wire.read_data_chunk().await?;
                    if chunk.is_empty() || chunk.len() as u64 > remaining {
                        return Err(invalid("invalid streamed file length"));
                    }
                    // Await each write: no detached writer can outlive cancellation
                    // cleanup of the temporary pathname.
                    file.write_all(&chunk).await?;
                    file.flush().await?;
                    context.emit(SyncEvent::Progress {
                        sent_bytes: wire.sent,
                        received_bytes: wire.received,
                    });
                    hash.update(&chunk);
                    remaining -= chunk.len() as u64;
                }
                file.flush().await?;
                drop(file);
                let expected = match wire.receive().await? {
                    Message::ContentEnd { digest } if digest.len() == 64 => digest,
                    _ => return Err(invalid("expected full-file digest")),
                };
                if hash.finalize().to_hex().as_str() != expected {
                    drop(temp);
                    if attempt == 0 {
                        context.emit(SyncEvent::Retrying {
                            path: entry.path.path()?,
                            attempt: 2,
                        });
                        continue;
                    }
                    return Err(SyncError::VerificationFailed {
                        path: entry.path.path()?,
                    });
                }
                fs::check_cancel(&context.cancel)?;
                let p = path.to_owned();
                let e = entry.clone();
                let o = options.clone();
                blocking(move || fs::commit_stage(temp, &p, &e, &o)).await?;
                report.committed_changes += 1;
                wire.send(&Message::ItemResult { committed: true }).await?;
                return Ok(());
            }
            let data = wire
                .receive_data(bytes, self.limits.max_buffer_bytes / 8)
                .await?;
            let engine = self.engine.clone();
            let base = basis.clone();
            let cancel = context.cancel.clone();
            let decoded = blocking(move || {
                fs::check_cancel(&cancel)?;
                let output = if delta {
                    let mut out = Vec::new();
                    let ctx = DeltaContext::new(&cancel, budget, file_len);
                    engine.patch(
                        &mut Cursor::new(base.as_slice()),
                        base.len() as u64,
                        &mut &data[..],
                        &mut out,
                        &ctx,
                    )?;
                    out
                } else {
                    data
                };
                if output.len() as u64 != file_len
                    || blake3::hash(&output).to_hex().as_str() != digest
                {
                    return Ok(None);
                }
                Ok(Some(output))
            })
            .await;
            match decoded {
                Ok(Some(output)) => {
                    fs::check_cancel(&context.cancel)?;
                    let p = path.to_owned();
                    let e = entry.clone();
                    let o = options.clone();
                    blocking(move || fs::stage_file(&p, &e, &o, &output)).await?;
                    report.committed_changes += 1;
                    wire.send(&Message::ItemResult { committed: true }).await?;
                    return Ok(());
                }
                Err(SyncError::Cancelled) => return Err(SyncError::Cancelled),
                Err(error) if !matches!(error, SyncError::Delta(_)) => return Err(error),
                _ if attempt == 0 => context.emit(SyncEvent::Retrying {
                    path: entry.path.path()?,
                    attempt: 2,
                }),
                _ => {
                    return Err(SyncError::VerificationFailed {
                        path: entry.path.path()?,
                    });
                }
            }
        }
        unreachable!("two attempts return or fail")
    }

    #[allow(clippy::too_many_arguments)]
    async fn delete_directory<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send>(
        &self,
        _wire: &mut Wire<'_, R, W>,
        root: &Path,
        relative: &Path,
        expected: &Arc<HashSet<PathBuf>>,
        filter: &Arc<Filter>,
        options: &SyncOptions,
        context: &mut SyncContext<'_>,
        report: &mut SyncReport,
    ) -> Result<(), SyncError> {
        if !options.delete || report.failed_items != 0 {
            return Ok(());
        }
        context.emit(SyncEvent::PhaseChanged(Phase::Delete));
        let root = root.to_owned();
        let relative = relative.to_owned();
        let expected = expected.clone();
        let filter = filter.clone();
        let dry = options.dry_run;
        let cancel = context.cancel.clone();
        let max = self.limits.max_entries;
        let deleted = blocking(move || {
            delete_extras(&root, &relative, &expected, &filter, dry, &cancel, max)
        })
        .await;
        match deleted {
            Ok(paths) => {
                for path in paths {
                    report.planned_changes += 1;
                    if !dry {
                        report.committed_changes += 1;
                    }
                    context.emit(SyncEvent::ItemFinished {
                        path,
                        action: ItemAction::Delete,
                    });
                }
            }
            Err(SyncError::Cancelled) => return Err(SyncError::Cancelled),
            Err(error) => self.issue(
                report,
                context,
                SyncIssue {
                    path: None,
                    operation: IssueOperation::Delete,
                    message: error.to_string(),
                },
            ),
        }
        context.emit(SyncEvent::PhaseChanged(Phase::Transfer));
        Ok(())
    }

    fn issue(&self, report: &mut SyncReport, context: &mut SyncContext<'_>, issue: SyncIssue) {
        report.failed_items += 1;
        context.emit(SyncEvent::Issue(issue.clone()));
        if report.issues.len() < self.limits.max_reported_issues {
            report.issues.push(issue);
        } else {
            report.omitted_issues += 1;
        }
    }
}

async fn target_path(root: &Path, relative: &Path) -> Result<PathBuf, SyncError> {
    let root = root.to_owned();
    let relative = relative.to_owned();
    blocking(move || fs::target(&root, &relative)).await
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, SyncError> + Send + 'static,
) -> Result<T, SyncError> {
    // Await rather than abort blocking work: its owned temporary files must drop
    // before cooperative cancellation can promise cleanup.
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|e| invalid(format!("worker failed: {e}")))?
}

fn delete_extras(
    root: &Path,
    relative: &Path,
    expected: &HashSet<PathBuf>,
    filter: &Filter,
    dry: bool,
    cancel: &CancellationToken,
    max: usize,
) -> Result<Vec<PathBuf>, SyncError> {
    fn visit(
        root: &Path,
        rel: &Path,
        filter: &Filter,
        dry: bool,
        cancel: &CancellationToken,
        out: &mut Vec<PathBuf>,
        max: usize,
    ) -> Result<bool, SyncError> {
        fs::check_cancel(cancel)?;
        let path = fs::target(root, rel)?;
        let m = std::fs::symlink_metadata(&path)?;
        if !filter.included(rel, m.is_dir()) {
            return Ok(false);
        }
        let mut removable = true;
        if m.is_dir() && !m.file_type().is_symlink() {
            for child in std::fs::read_dir(&path)? {
                let child = child?;
                let next = rel.join(child.file_name());
                WirePath::from_path(&next)?.path()?;
                removable &= visit(root, &next, filter, dry, cancel, out, max)?;
            }
        }
        if removable {
            if out.len() >= max {
                return Err(SyncError::LimitExceeded {
                    resource: Resource::Entries,
                });
            }
            if !dry {
                fs::remove(&path)?;
            }
            out.push(rel.to_owned());
        }
        Ok(removable)
    }
    let directory = fs::target(root, relative)?;
    let children = match std::fs::read_dir(directory) {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound && dry => return Ok(Vec::new()),
        Err(e) => return Err(e.into()),
    };
    let mut removed = Vec::new();
    for child in children {
        let child = child?;
        let rel = relative.join(child.file_name());
        WirePath::from_path(&rel)?.path()?;
        if !expected.contains(&rel) {
            visit(root, &rel, filter, dry, cancel, &mut removed, max)?;
        }
    }
    Ok(removed)
}
