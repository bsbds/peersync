use peersync::{delta::*, *};
use std::{
    io::{Read, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct CorruptDelta {
    calls: AtomicUsize,
    always: bool,
    hashes: Arc<Mutex<Vec<u32>>>,
}
impl DeltaEngine for CorruptDelta {
    fn format(&self) -> DeltaFormat {
        FastRsync.format()
    }
    fn validate_options(&self, options: &SignatureOptions) -> Result<(), DeltaError> {
        FastRsync.validate_options(options)
    }
    fn signature(
        &self,
        basis: &mut dyn ReadSeek,
        len: u64,
        options: &SignatureOptions,
        out: &mut dyn Write,
        ctx: &DeltaContext<'_>,
    ) -> Result<(), DeltaError> {
        FastRsync.signature(basis, len, options, out, ctx)
    }
    fn delta(
        &self,
        signature: &mut dyn Read,
        source: &mut dyn ReadSeek,
        len: u64,
        out: &mut dyn Write,
        ctx: &DeltaContext<'_>,
    ) -> Result<(), DeltaError> {
        let mut sig = Vec::new();
        signature.read_to_end(&mut sig)?;
        self.hashes
            .lock()
            .unwrap()
            .push(u32::from_be_bytes(sig[8..12].try_into().unwrap()));
        let mut input = Vec::new();
        source.read_to_end(&mut input)?;
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 || self.always {
            input[0] ^= 1;
        }
        // Valid delta, wrong reconstructed content: exercise BLAKE3 verification,
        // not merely a parser rejecting a corrupted magic header.
        let mut delta = Vec::new();
        FastRsync.delta(
            &mut &sig[..],
            &mut std::io::Cursor::new(input),
            len,
            &mut delta,
            ctx,
        )?;
        out.write_all(&delta)?;
        Ok(())
    }
    fn patch(
        &self,
        basis: &mut dyn ReadSeek,
        len: u64,
        delta: &mut dyn Read,
        out: &mut dyn Write,
        ctx: &DeltaContext<'_>,
    ) -> Result<(), DeltaError> {
        FastRsync.patch(basis, len, delta, out, ctx)
    }
}

async fn exercise(always: bool) {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    std::fs::write(src.path().join("file"), vec![42; 8192]).unwrap();
    std::fs::write(dst.path().join("file"), vec![41; 8192]).unwrap();
    let hashes = Arc::new(Mutex::new(Vec::new()));
    let sender = Syncer::with_engine(CorruptDelta {
        calls: AtomicUsize::new(0),
        always,
        hashes: hashes.clone(),
    });
    let receiver = Syncer::new();
    let (a, b) = tokio::io::duplex(31);
    let (mut ar, mut aw) = tokio::io::split(a);
    let (mut br, mut bw) = tokio::io::split(b);
    let mut options = SyncOptions::default();
    options.comparison = Comparison::Checksum;
    let mut sc = SyncContext::new(CancellationToken::new());
    let mut rc = SyncContext::new(CancellationToken::new());
    let (a, b) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            async {
                let r = sender
                    .send(&mut ar, &mut aw, src.path(), &options, &mut sc)
                    .await;
                drop(ar);
                drop(aw);
                r
            },
            async {
                let r = receiver
                    .receive(&mut br, &mut bw, dst.path(), None, &mut rc)
                    .await;
                drop(br);
                drop(bw);
                r
            }
        )
    })
    .await
    .unwrap();
    let values = hashes.lock().unwrap();
    assert_eq!(values.len(), 2);
    assert_eq!(values[1], 16);
    if always {
        assert!(a.is_err());
        assert!(b.is_err());
        assert_eq!(
            std::fs::read(dst.path().join("file")).unwrap(),
            vec![41; 8192]
        );
    } else {
        a.unwrap();
        b.unwrap();
        assert_eq!(
            std::fs::read(dst.path().join("file")).unwrap(),
            vec![42; 8192]
        );
    }
}
#[tokio::test]
async fn redo_uses_full_hash_without_whole_file_fallback() {
    exercise(false).await;
}
#[tokio::test]
async fn two_failed_attempts_leave_original_file_unchanged() {
    exercise(true).await;
}
