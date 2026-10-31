use peersync::*;
use serde_json::{Value, json};
use std::{path::Path, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

async fn control(stream: &mut DuplexStream, message: Value) {
    let data = serde_json::to_vec(&message).unwrap();
    stream.write_u8(1).await.unwrap();
    stream.write_u32(data.len() as u32).await.unwrap();
    stream.write_all(&data).await.unwrap();
}
async fn receive(stream: &mut DuplexStream) -> Value {
    assert_eq!(stream.read_u8().await.unwrap(), 1);
    let len = stream.read_u32().await.unwrap();
    let mut bytes = vec![0; len as usize];
    stream.read_exact(&mut bytes).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}
async fn hello(stream: &mut DuplexStream) {
    let mut options = SyncOptions::default();
    options.transfer = TransferMode::WholeFile;
    control(stream,json!({"type":"Hello","version":2,"signature":"librsync-md4","delta":"librsync-delta","format_version":1,"options":options})).await;
    assert_eq!(receive(stream).await["type"], "Ready");
}
fn entry(path: Vec<Vec<u8>>, kind: &str, len: u64) -> Value {
    json!({"type":"Entry","path":path,"kind":kind,"len":len,"mtime":1700000000,"nanos":0,"mode":420,"digest":null,"link":null})
}
async fn receiver(stream: DuplexStream, path: &Path) -> SyncResult {
    let (mut r, mut w) = tokio::io::split(stream);
    let syncer = Syncer::new();
    let mut ctx = SyncContext::new(CancellationToken::new());
    syncer.receive(&mut r, &mut w, path, None, &mut ctx).await
}

#[tokio::test]
async fn old_version_or_filter_shape_fails_before_destination_creation() {
    for old_version in [true, false] {
        let root = tempfile::tempdir().unwrap();
        let target = root.path().join("absent");
        let mut options = serde_json::to_value(SyncOptions::default()).unwrap();
        if !old_version {
            options["filter"] = json!("None");
        }
        let (mut peer, local) = tokio::io::duplex(4096);
        let ((), result) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(async {
                control(&mut peer,json!({"type":"Hello","version":if old_version {1} else {2},
                    "signature":"librsync-md4","delta":"librsync-delta","format_version":1,"options":options})).await;
            }, receiver(local,&target))
        }).await.unwrap();
        assert!(result.is_err());
        assert!(!target.exists());
    }
}

#[tokio::test]
async fn invalid_or_cancelled_filter_decisions_do_not_delete() {
    for cancelled in [false, true] {
        let target = tempfile::tempdir().unwrap();
        std::fs::write(target.path().join("extra"), b"safe").unwrap();
        let mut options = SyncOptions::default();
        options.delete = true;
        let cancellation = CancellationToken::new();
        let (mut peer, local) = tokio::io::duplex(4096);
        let (mut read, mut write) = tokio::io::split(local);
        let syncer = Syncer::new();
        let mut context = SyncContext::new(cancellation.clone());
        let ((), result) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(async {
                control(&mut peer,json!({"type":"Hello","version":2,"signature":"librsync-md4","delta":"librsync-delta","format_version":1,"options":options})).await;
                assert_eq!(receive(&mut peer).await["type"],"Ready");
                control(&mut peer,entry(vec![],"Directory",0)).await;
                control(&mut peer,json!({"type":"ManifestEnd"})).await;
                assert_eq!(receive(&mut peer).await["type"],"FilterQuery");
                if cancelled { cancellation.cancel(); }
                else { control(&mut peer,json!({"type":"FilterResult","included":[]})).await; }
            }, syncer.receive(&mut read,&mut write,target.path(),Some(&options),&mut context))
        }).await.unwrap();
        assert!(result.is_err());
        assert_eq!(std::fs::read(target.path().join("extra")).unwrap(), b"safe");
    }
}

#[tokio::test]
async fn source_rejects_deletion_candidate_traversal() {
    let source = tempfile::tempdir().unwrap();
    let mut options = SyncOptions::default();
    options.delete = true;
    let (mut peer, local) = tokio::io::duplex(4096);
    let (mut read, mut write) = tokio::io::split(local);
    let syncer = Syncer::new();
    let mut context = SyncContext::new(CancellationToken::new());
    let ((), result) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(async {
            assert_eq!(receive(&mut peer).await["type"],"Hello");
            control(&mut peer,json!({"type":"Ready"})).await;
            while receive(&mut peer).await["type"] != "ManifestEnd" {}
            control(&mut peer,json!({"type":"FilterQuery","candidates":[{"path":[[46,46],[120]],"is_dir":false}]})).await;
        }, syncer.send(&mut read,&mut write,source.path(),&options,&mut context))
    }).await.unwrap();
    assert!(matches!(
        result.unwrap_err().error,
        SyncError::Protocol { .. }
    ));
}

#[tokio::test]
async fn traversal_is_rejected_before_destination_creation() {
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("target");
    let outside = dir.path().join("outside");
    std::fs::write(&outside, "safe").unwrap();
    let (mut peer, local) = tokio::io::duplex(4096);
    let ((), result) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            async {
                hello(&mut peer).await;
                control(&mut peer, entry(vec![], "Directory", 0)).await;
                control(
                    &mut peer,
                    entry(vec![b"..".to_vec(), b"outside".to_vec()], "File", 4),
                )
                .await;
                control(&mut peer, json!({"type":"ManifestEnd"})).await;
            },
            receiver(local, &target)
        )
    })
    .await
    .unwrap();
    assert!(result.is_err());
    assert!(!target.exists());
    assert_eq!(std::fs::read(outside).unwrap(), b"safe");
}

#[tokio::test]
async fn oversized_frame_rejected_before_payload_allocation() {
    let dir = tempfile::tempdir().unwrap();
    let (mut peer, local) = tokio::io::duplex(32);
    let ((), result) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            async {
                peer.write_u8(1).await.unwrap();
                peer.write_u32(u32::MAX).await.unwrap();
            },
            receiver(local, dir.path())
        )
    })
    .await
    .unwrap();
    assert!(matches!(
        result.unwrap_err().error,
        SyncError::LimitExceeded { .. }
    ));
}

#[tokio::test]
async fn truncated_stream_cleans_temporary_and_keeps_old_file() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), "old").unwrap();
    let (mut peer, local) = tokio::io::duplex(4096);
    let ((), result) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            async {
                hello(&mut peer).await;
                control(&mut peer, entry(vec![], "Directory", 0)).await;
                control(&mut peer, entry(vec![b"file".to_vec()], "File", 8)).await;
                control(&mut peer, json!({"type":"ManifestEnd"})).await;
                assert_eq!(receive(&mut peer).await["type"], "Request");
                control(
                    &mut peer,
                    json!({"type":"Content","bytes":8,"file_len":8,"digest":"","delta":false}),
                )
                .await;
                peer.write_u8(2).await.unwrap();
                peer.write_u32(4).await.unwrap();
                peer.write_all(b"part").await.unwrap();
                peer.shutdown().await.unwrap();
            },
            receiver(local, dir.path())
        )
    })
    .await
    .unwrap();
    assert!(result.is_err());
    assert_eq!(std::fs::read(dir.path().join("file")).unwrap(), b"old");
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[tokio::test]
async fn concurrent_roles_on_same_syncer_fail_instead_of_deadlocking() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Syncer::new();
    let (a, b) = tokio::io::duplex(1024);
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let mut sc = SyncContext::new(CancellationToken::new());
    let mut rc = SyncContext::new(CancellationToken::new());
    let options = SyncOptions::default();
    let (a, b) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            async {
                let r = engine
                    .send(&mut ar, &mut aw, dir.path(), &options, &mut sc)
                    .await;
                drop(ar);
                drop(aw);
                r
            },
            async {
                let r = engine
                    .receive(&mut br, &mut bw, dir.path(), None, &mut rc)
                    .await;
                drop(br);
                drop(bw);
                r
            }
        )
    })
    .await
    .unwrap();
    assert!(a.is_err());
    assert!(b.is_err());
}
