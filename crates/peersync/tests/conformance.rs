//! Opt-in external oracle: PEERSYNC_RSYNC must point to exactly rsync 3.4.1.
//! This test dependency is never invoked by the library or shipped CLI.
#![cfg(unix)]
use peersync::*;
use std::{
    collections::BTreeMap, os::unix::fs::PermissionsExt, path::Path, process::Command,
    time::Duration,
};

fn tree(root: &Path, metadata: bool) -> BTreeMap<std::path::PathBuf, (String, Vec<u8>, u32, i64)> {
    fn visit(
        root: &Path,
        relative: &Path,
        out: &mut BTreeMap<std::path::PathBuf, (String, Vec<u8>, u32, i64)>,
        metadata: bool,
    ) {
        let p = root.join(relative);
        let m = std::fs::symlink_metadata(&p).unwrap();
        let (kind, data) = if m.file_type().is_symlink() {
            (
                "link",
                std::fs::read_link(&p)
                    .unwrap()
                    .as_os_str()
                    .as_encoded_bytes()
                    .to_vec(),
            )
        } else if m.is_dir() {
            ("dir", Vec::new())
        } else {
            ("file", std::fs::read(&p).unwrap())
        };
        out.insert(
            relative.to_owned(),
            (
                kind.into(),
                data,
                m.permissions().mode() & 0o777,
                if metadata {
                    filetime::FileTime::from_last_modification_time(&m).unix_seconds()
                } else {
                    0
                },
            ),
        );
        if m.is_dir() {
            for entry in std::fs::read_dir(p).unwrap() {
                let entry = entry.unwrap();
                visit(root, &relative.join(entry.file_name()), out, metadata);
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, Path::new(""), &mut out, metadata);
    out
}
fn populate(root: &Path, source: bool) {
    std::fs::create_dir(root.join("nested")).unwrap();
    std::fs::create_dir(root.join("empty")).unwrap();
    std::fs::write(
        root.join("nested/file"),
        if source { "new contents" } else { "old" },
    )
    .unwrap();
    std::fs::write(root.join(".hidden"), "hidden").unwrap();
    std::fs::set_permissions(
        root.join("nested/file"),
        std::fs::Permissions::from_mode(if source { 0o740 } else { 0o600 }),
    )
    .unwrap();
    std::fs::set_permissions(root.join("empty"), std::fs::Permissions::from_mode(0o700)).unwrap();
    if source {
        std::fs::write(root.join("new"), "new").unwrap();
        std::os::unix::fs::symlink("nested/file", root.join("link")).unwrap();
    } else {
        std::fs::write(root.join("extra"), "extra").unwrap();
        std::fs::write(root.join("link"), "old link path").unwrap();
    }
    for p in [
        root.to_owned(),
        root.join("nested"),
        root.join("empty"),
        root.join("nested/file"),
        root.join(".hidden"),
    ] {
        filetime::set_file_mtime(
            p,
            filetime::FileTime::from_unix_time(if source { 1700000000 } else { 1600000000 }, 0),
        )
        .unwrap();
    }
}
#[tokio::test]
#[ignore = "requires explicit PEERSYNC_RSYNC=path/to/rsync-3.4.1"]
async fn recursive_subset_matches_pinned_rsync() {
    let rsync = std::env::var_os("PEERSYNC_RSYNC").expect("set reference binary");
    let version = Command::new(&rsync).arg("--version").output().unwrap();
    assert!(String::from_utf8_lossy(&version.stdout).starts_with("rsync  version 3.4.1 "));
    for preserve in [false, true] {
        for delete in [false, true] {
            let source = tempfile::tempdir().unwrap();
            let expected = tempfile::tempdir().unwrap();
            let actual = tempfile::tempdir().unwrap();
            populate(source.path(), true);
            populate(expected.path(), false);
            populate(actual.path(), false);
            let mut cmd = Command::new(&rsync);
            cmd.arg("-r");
            if preserve {
                cmd.args(["-t", "-p", "-l"]);
            }
            if delete {
                cmd.arg("--delete");
            }
            let result = cmd
                .arg(format!("{}/", source.path().display()))
                .arg(format!("{}/", expected.path().display()))
                .output()
                .unwrap();
            assert!(
                result.status.success(),
                "{}",
                String::from_utf8_lossy(&result.stderr)
            );
            let mut options = SyncOptions::default();
            options.delete = delete;
            options.preserve_mtime = preserve;
            options.preserve_permissions = preserve;
            options.symlinks = if preserve {
                SymlinkMode::Preserve
            } else {
                SymlinkMode::Skip
            };
            let (a, b) = tokio::io::duplex(1024);
            let (mut ar, mut aw) = tokio::io::split(a);
            let (mut br, mut bw) = tokio::io::split(b);
            let sender = Syncer::new();
            let receiver = Syncer::new();
            let mut sc = SyncContext::new(CancellationToken::new());
            let mut rc = SyncContext::new(CancellationToken::new());
            let (a, b) = tokio::time::timeout(Duration::from_secs(10), async {
                tokio::join!(
                    sender.send(&mut ar, &mut aw, source.path(), &options, &mut sc),
                    receiver.receive(&mut br, &mut bw, actual.path(), None, &mut rc)
                )
            })
            .await
            .unwrap();
            a.unwrap();
            b.unwrap();
            assert_eq!(
                tree(actual.path(), preserve),
                tree(expected.path(), preserve),
                "preserve={preserve},delete={delete}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires explicit PEERSYNC_RSYNC=path/to/rsync-3.4.1"]
async fn deletion_error_timing_and_directory_to_file_conversion() {
    let rsync = std::env::var_os("PEERSYNC_RSYNC").expect("set reference binary");
    for scan_error in [false, true] {
        let source = tempfile::tempdir().unwrap();
        let expected = tempfile::tempdir().unwrap();
        let actual = tempfile::tempdir().unwrap();
        std::fs::write(source.path().join("file"), "new").unwrap();
        if scan_error {
            std::fs::create_dir(source.path().join("bad")).unwrap();
            std::fs::set_permissions(
                source.path().join("bad"),
                std::fs::Permissions::from_mode(0o0),
            )
            .unwrap();
        }
        for dst in [expected.path(), actual.path()] {
            std::fs::write(dst.join("extra"), "delete").unwrap();
            std::fs::create_dir(dst.join("file")).unwrap();
            std::fs::write(dst.join("file/old"), "old").unwrap();
        }
        let reference = Command::new(&rsync)
            .args(["-r", "--delete"])
            .arg(format!("{}/", source.path().display()))
            .arg(format!("{}/", expected.path().display()))
            .output()
            .unwrap();
        let (a, b) = tokio::io::duplex(1024);
        let (mut ar, mut aw) = tokio::io::split(a);
        let (mut br, mut bw) = tokio::io::split(b);
        let sender = Syncer::new();
        let receiver = Syncer::new();
        let mut options = SyncOptions::default();
        options.delete = true;
        let mut sc = SyncContext::new(CancellationToken::new());
        let mut rc = SyncContext::new(CancellationToken::new());
        let (sent, received) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(
                async {
                    let r = sender
                        .send(&mut ar, &mut aw, source.path(), &options, &mut sc)
                        .await;
                    drop(ar);
                    drop(aw);
                    r
                },
                async {
                    let r = receiver
                        .receive(&mut br, &mut bw, actual.path(), None, &mut rc)
                        .await;
                    drop(br);
                    drop(bw);
                    r
                }
            )
        })
        .await
        .unwrap();
        if scan_error {
            std::fs::set_permissions(
                source.path().join("bad"),
                std::fs::Permissions::from_mode(0o700),
            )
            .unwrap();
            // Make destination trees readable for the assertion, not the sync.
            for dst in [expected.path(), actual.path()] {
                if dst.join("bad").exists() {
                    std::fs::set_permissions(
                        dst.join("bad"),
                        std::fs::Permissions::from_mode(0o700),
                    )
                    .unwrap();
                }
            }
        }
        assert_eq!(reference.status.success(), sent.is_ok());
        assert_eq!(reference.status.success(), received.is_ok());
        assert_eq!(
            tree(actual.path(), false),
            tree(expected.path(), false),
            "scan_error={scan_error}; rsync stdout={} stderr={}",
            String::from_utf8_lossy(&reference.stdout),
            String::from_utf8_lossy(&reference.stderr)
        );
    }
}
