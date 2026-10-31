use peersync::*;
use std::{path::Path, time::Duration};

async fn sync(source: &Path, target: &Path, options: &SyncOptions) -> (SyncResult, SyncResult) {
    fn sendable<T: Send>(future: T) -> T {
        future
    }
    let (a, b) = tokio::io::duplex(31);
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let sender = Syncer::new();
    let receiver = Syncer::new();
    let mut send_context = SyncContext::new(CancellationToken::new());
    let mut receive_context = SyncContext::new(CancellationToken::new());
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            sendable(sender.send(&mut ar, &mut aw, source, options, &mut send_context)),
            sendable(receiver.receive(
                &mut br,
                &mut bw,
                target,
                Some(options),
                &mut receive_context
            )),
        )
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn source_rules_protect_deletion_independently_of_target_rules() {
    for dry_run in [true, false] {
        let source = tempfile::tempdir().unwrap();
        let target = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join(".ignore"), "*.tmp\nprotected/\n").unwrap();
        std::fs::write(source.path().join("visible"), b"source").unwrap();
        std::fs::write(source.path().join(".hidden"), b"hidden").unwrap();
        std::fs::write(target.path().join(".ignore"), "*\n").unwrap();
        std::fs::create_dir(target.path().join("protected")).unwrap();
        for name in ["extra", "keep.tmp", ".local", "protected/child"] {
            std::fs::write(target.path().join(name), b"keep").unwrap();
        }
        let mut options = SyncOptions::default();
        options.delete = true;
        options.dry_run = dry_run;
        let (sent, received) = sync(source.path(), target.path(), &options).await;
        sent.unwrap();
        received.unwrap();
        assert_eq!(target.path().join("extra").exists(), dry_run);
        assert_eq!(target.path().join("visible").exists(), !dry_run);
        for name in ["keep.tmp", ".local", "protected/child", ".ignore"] {
            assert!(target.path().join(name).exists(), "{name}");
        }
        assert!(!target.path().join(".hidden").exists());
    }
}

#[tokio::test]
async fn whitelist_overrides_ignore_and_last_rule_controls_deletion() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join(".ignore"), "*.rs\n").unwrap();
    for name in ["keep.rs", ".hidden.rs", "blocked.rs", "other.txt"] {
        std::fs::write(source.path().join(name), b"source").unwrap();
    }
    for name in ["extra.rs", "extra.txt", "blocked.rs"] {
        std::fs::write(target.path().join(name), b"target").unwrap();
    }
    let mut options = SyncOptions::default();
    options.delete = true;
    options.filter.globs = vec!["*.rs".into(), "!blocked.rs".into()];
    let (sent, received) = sync(source.path(), target.path(), &options).await;
    sent.unwrap();
    received.unwrap();
    assert!(target.path().join("keep.rs").exists());
    assert!(target.path().join(".hidden.rs").exists());
    assert!(!target.path().join("other.txt").exists());
    assert!(!target.path().join("extra.rs").exists());
    assert!(target.path().join("extra.txt").exists());
    assert_eq!(
        std::fs::read(target.path().join("blocked.rs")).unwrap(),
        b"target"
    );
}

#[tokio::test]
async fn protected_child_prevents_directory_to_file_replacement() {
    let source = tempfile::tempdir().unwrap();
    let target = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join(".ignore"), "*.tmp\n").unwrap();
    std::fs::write(source.path().join("replace"), b"new file").unwrap();
    std::fs::create_dir(target.path().join("replace")).unwrap();
    std::fs::write(target.path().join("replace/protected.tmp"), b"protected").unwrap();
    let mut options = SyncOptions::default();
    options.delete = true;
    let (_, received) = sync(source.path(), target.path(), &options).await;
    assert!(received.is_err());
    assert_eq!(
        std::fs::read(target.path().join("replace/protected.tmp")).unwrap(),
        b"protected"
    );
}
