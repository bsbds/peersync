//! Manual isolated RSS/time probe. No network, no external rsync, no giant input
//! allocation in the harness. Arguments: FILE_BYTES BUFFER_BUDGET_BYTES.
use peersync::*;
use std::{io::Write, time::Instant};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let bytes: u64 = args.get(1).ok_or("file size required")?.parse()?;
    let budget: usize = args.get(2).ok_or("buffer budget required")?.parse()?;
    let source = tempfile::tempdir()?;
    let target = tempfile::tempdir()?;
    for (root, byte) in [(source.path(), 42u8), (target.path(), 41u8)] {
        let mut file = std::fs::File::create(root.join("file"))?;
        let chunk = [byte; 65536];
        let mut remaining = bytes;
        while remaining > 0 {
            let size = remaining.min(chunk.len() as u64) as usize;
            file.write_all(&chunk[..size])?;
            remaining -= size as u64;
        }
    }
    let mut limits = Limits::default();
    limits.max_buffer_bytes = budget;
    limits.max_control_frame_bytes = 64 * 1024;
    limits.max_data_frame_bytes = 16 * 1024;
    let sender = Syncer::new().with_limits(limits.clone())?;
    let receiver = Syncer::new().with_limits(limits)?;
    let mut options = SyncOptions::default();
    options.comparison = Comparison::Checksum;
    let (a, b) = tokio::io::duplex(32768);
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let mut sc = SyncContext::new(CancellationToken::new());
    let mut rc = SyncContext::new(CancellationToken::new());
    let start = Instant::now();
    let (a, b) = tokio::join!(
        sender.send(&mut ar, &mut aw, source.path(), &options, &mut sc),
        receiver.receive(&mut br, &mut bw, target.path(), None, &mut rc)
    );
    let a = a?;
    b?;
    println!(
        "file_bytes={bytes} budget_per_role={budget} sent_bytes={} elapsed_ms={}",
        a.sent_bytes,
        start.elapsed().as_millis()
    );
    Ok(())
}
