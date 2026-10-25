use peersync::*;
use std::time::Duration;

#[tokio::test]
async fn files_larger_than_memory_budget_stream_in_chunks() {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let data = vec![42u8; 4 * 1024 * 1024];
    std::fs::write(src.path().join("file"), &data).unwrap();
    std::fs::write(dst.path().join("file"), vec![41u8; data.len()]).unwrap();
    let mut limits = Limits::default();
    limits.max_buffer_bytes = 1024 * 1024;
    limits.max_control_frame_bytes = 64 * 1024;
    limits.max_data_frame_bytes = 8192;
    let sender = Syncer::new().with_limits(limits.clone()).unwrap();
    let receiver = Syncer::new().with_limits(limits).unwrap();
    let mut options = SyncOptions::default();
    options.comparison = Comparison::Checksum;
    let (a, b) = tokio::io::duplex(1024);
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let mut sc = SyncContext::new(CancellationToken::new());
    let mut rc = SyncContext::new(CancellationToken::new());
    let (a, b) = tokio::time::timeout(Duration::from_secs(15), async {
        tokio::join!(
            sender.send(&mut ar, &mut aw, src.path(), &options, &mut sc),
            receiver.receive(&mut br, &mut bw, dst.path(), &mut rc)
        )
    })
    .await
    .unwrap();
    a.unwrap();
    b.unwrap();
    assert_eq!(std::fs::read(dst.path().join("file")).unwrap(), data);
    assert_eq!(std::fs::read_dir(dst.path()).unwrap().count(), 1);
}
