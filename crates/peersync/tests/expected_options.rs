use peersync::{
    CancellationToken, Comparison, FilterMode, FilterRule, SymlinkMode, SyncContext, SyncError,
    SyncOptions, Syncer, TransferMode,
};

async fn run(sent: SyncOptions, expected: SyncOptions, success: bool) {
    let source = tempfile::tempdir().unwrap();
    std::fs::write(source.path().join("file"), b"content").unwrap();
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("absent");
    let (a, b) = tokio::io::duplex(64);
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let sender = Syncer::new();
    let receiver = Syncer::new();
    let mut sc = SyncContext::new(CancellationToken::new());
    let mut rc = SyncContext::new(CancellationToken::new());
    let (a, b) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            sender.send(&mut ar, &mut aw, source.path(), &sent, &mut sc),
            receiver.receive(&mut br, &mut bw, &destination, Some(&expected), &mut rc)
        )
    })
    .await
    .unwrap();
    if success {
        a.unwrap();
        b.unwrap();
        assert_eq!(std::fs::read(destination.join("file")).unwrap(), b"content");
    } else {
        assert!(a.is_err());
        assert!(matches!(b.unwrap_err().error, SyncError::OptionsMismatch));
        assert!(
            !destination.exists(),
            "mismatched options must have no filesystem effects"
        );
    }
}

#[tokio::test]
async fn approved_options_work_and_every_field_is_checked_before_mutation() {
    let defaults = SyncOptions::default();
    run(defaults.clone(), defaults.clone(), true).await;
    let mut cases = Vec::new();
    for field in 0..8 {
        let mut changed = defaults.clone();
        match field {
            0 => changed.delete = true,
            1 => changed.dry_run = true,
            2 => changed.comparison = Comparison::Checksum,
            3 => changed.transfer = TransferMode::WholeFile,
            4 => changed.preserve_mtime = true,
            5 => changed.preserve_permissions = true,
            6 => changed.symlinks = SymlinkMode::Preserve,
            _ => changed.filter = FilterMode::GitIgnore,
        }
        cases.push(changed);
    }
    for changed in cases {
        run(changed.clone(), defaults.clone(), false).await;
        run(defaults.clone(), changed, false).await;
    }
    let mut expected = defaults.clone();
    expected.filter = FilterMode::Glob(vec![
        FilterRule::Include("a".into()),
        FilterRule::Exclude("*".into()),
    ]);
    let mut sent = expected.clone();
    if let FilterMode::Glob(rules) = &mut sent.filter {
        rules.reverse();
    }
    run(sent, expected, false).await;
}
