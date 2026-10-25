use peersync::*;
use std::{path::Path, time::Duration};

async fn sync(src: &Path, dst: &Path, options: SyncOptions) -> (SyncResult, SyncResult) {
    let sender = Syncer::new();
    let receiver = Syncer::new();
    let (a, b) = tokio::io::duplex(17);
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let mut sc = SyncContext::new(CancellationToken::new());
    let mut rc = SyncContext::new(CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            async {
                let r = sender.send(&mut ar, &mut aw, src, &options, &mut sc).await;
                drop(ar);
                drop(aw);
                r
            },
            async {
                let r = receiver.receive(&mut br, &mut bw, dst, &mut rc).await;
                drop(br);
                drop(bw);
                r
            }
        )
    })
    .await
    .expect("protocol hung")
}

#[tokio::test]
async fn creates_updates_and_skips_with_small_stream_buffers() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("empty")).unwrap();
    std::fs::write(src.path().join("file"), vec![42; 8192]).unwrap();
    std::fs::write(dst.path().join("file"), vec![41; 8192]).unwrap();
    let mut options = SyncOptions::default();
    options.preserve_mtime = true;
    options.comparison = Comparison::Checksum;
    let (a, b) = sync(src.path(), dst.path(), options.clone()).await;
    a.unwrap();
    b.unwrap();
    assert_eq!(
        std::fs::read(dst.path().join("file")).unwrap(),
        vec![42; 8192]
    );
    assert!(dst.path().join("empty").is_dir());
    let (a, b) = sync(src.path(), dst.path(), options).await;
    a.unwrap();
    assert_eq!(b.unwrap().skipped_items, 1);
}

#[tokio::test]
async fn default_ignores_no_hidden_files_and_delete_is_opt_in() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join(".git")).unwrap();
    std::fs::write(src.path().join(".git/config"), "config").unwrap();
    std::fs::write(src.path().join(".gitignore"), "hidden\n").unwrap();
    std::fs::write(src.path().join("hidden"), "data").unwrap();
    std::fs::write(dst.path().join("extra"), "old").unwrap();
    let (a, b) = sync(src.path(), dst.path(), SyncOptions::default()).await;
    a.unwrap();
    b.unwrap();
    assert!(dst.path().join(".git/config").is_file());
    assert!(dst.path().join("hidden").exists());
    assert!(dst.path().join("extra").exists());
    let mut options = SyncOptions::default();
    options.delete = true;
    let (a, b) = sync(src.path(), dst.path(), options).await;
    a.unwrap();
    b.unwrap();
    assert!(!dst.path().join("extra").exists());
}

#[tokio::test]
async fn dry_run_does_not_create_destination_or_delete() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("d")).unwrap();
    std::fs::write(src.path().join("d/a"), "data").unwrap();
    let target = dst.path().join("missing");
    let mut options = SyncOptions::default();
    options.dry_run = true;
    options.delete = true;
    let (a, b) = sync(src.path(), &target, options).await;
    a.unwrap();
    let b = b.unwrap();
    assert_eq!(b.committed_changes, 0);
    assert!(!target.exists());
}

#[tokio::test]
async fn explicit_globs_prune_and_protect_excluded_files_from_delete() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("target")).unwrap();
    std::fs::write(src.path().join("target/a"), "skip").unwrap();
    std::fs::create_dir(dst.path().join("target")).unwrap();
    std::fs::write(dst.path().join("target/cache"), "keep").unwrap();
    std::fs::write(src.path().join("yes.rs"), "yes").unwrap();
    std::fs::write(dst.path().join("old.txt"), "delete").unwrap();
    let mut options = SyncOptions::default();
    options.delete = true;
    options.filter = FilterMode::Glob(vec![FilterRule::Exclude("target".into())]);
    let (a, b) = sync(src.path(), dst.path(), options).await;
    a.unwrap();
    b.unwrap();
    assert!(dst.path().join("target/cache").exists());
    assert!(!dst.path().join("target/a").exists());
    assert!(!dst.path().join("old.txt").exists());
}

#[tokio::test]
async fn gitignore_without_repository_and_nested_rules_protect_receiver() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join(".gitignore"), "*.tmp\n").unwrap();
    std::fs::create_dir(src.path().join("nested")).unwrap();
    std::fs::write(src.path().join("nested/.gitignore"), "!keep.tmp\n").unwrap();
    std::fs::write(src.path().join("nested/keep.tmp"), "keep").unwrap();
    std::fs::write(src.path().join("skip.tmp"), "skip").unwrap();
    std::fs::write(dst.path().join("extra.tmp"), "protected").unwrap();
    let mut options = SyncOptions::default();
    options.filter = FilterMode::GitIgnore;
    options.delete = true;
    let (a, b) = sync(src.path(), dst.path(), options).await;
    a.unwrap();
    b.unwrap();
    assert!(!dst.path().join("skip.tmp").exists());
    assert!(dst.path().join("nested/keep.tmp").exists());
    assert!(dst.path().join("extra.tmp").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn destination_dir_symlink_replaced_without_touching_outside() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("link")).unwrap();
    std::fs::write(src.path().join("link/file"), "new").unwrap();
    std::os::unix::fs::symlink(outside.path(), dst.path().join("link")).unwrap();
    let (a, b) = sync(src.path(), dst.path(), SyncOptions::default()).await;
    a.unwrap();
    b.unwrap();
    assert!(!dst.path().join("link").is_symlink());
    assert!(!outside.path().join("file").exists());
}

#[tokio::test]
async fn cancellation_returns_report_and_send_future_is_send() {
    fn is_send<T: Send>(_: &T) {}
    let src = tempfile::tempdir().unwrap();
    let (local, _peer) = tokio::io::duplex(1);
    let (mut r, mut w) = tokio::io::split(local);
    let token = CancellationToken::new();
    token.cancel();
    let syncer = Syncer::new();
    let options = SyncOptions::default();
    let mut context = SyncContext::new(token);
    let operation = syncer.send(&mut r, &mut w, src.path(), &options, &mut context);
    is_send(&operation);
    let failed = operation.await.unwrap_err();
    assert!(matches!(failed.error, SyncError::Cancelled));
    assert_eq!(failed.report.committed_changes, 0);
}

#[cfg(unix)]
#[tokio::test]
async fn non_utf8_names_roundtrip_without_lossy_conversion() {
    use std::os::unix::ffi::OsStringExt;
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let name = std::ffi::OsString::from_vec(vec![b'f', 0xff, b':', b'\\']);
    std::fs::write(src.path().join(&name), b"bytes").unwrap();
    let (a, b) = sync(src.path(), dst.path(), SyncOptions::default()).await;
    a.unwrap();
    b.unwrap();
    assert_eq!(std::fs::read(dst.path().join(name)).unwrap(), b"bytes");
}

#[cfg(unix)]
#[tokio::test]
async fn dry_run_never_follows_target_directory_symlink() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::create_dir(src.path().join("dir")).unwrap();
    std::fs::write(src.path().join("dir/file"), "new").unwrap();
    std::fs::write(outside.path().join("extra"), "safe").unwrap();
    std::os::unix::fs::symlink(outside.path(), dst.path().join("dir")).unwrap();
    let mut options = SyncOptions::default();
    options.dry_run = true;
    options.delete = true;
    let (a, b) = sync(src.path(), dst.path(), options).await;
    a.unwrap();
    b.unwrap();
    assert!(dst.path().join("dir").is_symlink());
    assert!(outside.path().join("extra").exists());
    assert!(!outside.path().join("file").exists());
}
