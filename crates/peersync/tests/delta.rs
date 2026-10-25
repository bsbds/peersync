use peersync::{CancellationToken, delta::*};
use std::io::Cursor;

fn roundtrip(old: Vec<u8>, new: Vec<u8>, hash: u8) {
    let cancel = CancellationToken::new();
    let context = DeltaContext::new(&cancel, 16 * 1024 * 1024, 1024 * 1024);
    let mut sig = Vec::new();
    FastRsync
        .signature(
            &mut Cursor::new(&old),
            old.len() as u64,
            &SignatureOptions::new(64, hash),
            &mut sig,
            &context,
        )
        .unwrap();
    let mut delta = Vec::new();
    FastRsync
        .delta(
            &mut &sig[..],
            &mut Cursor::new(&new),
            new.len() as u64,
            &mut delta,
            &context,
        )
        .unwrap();
    let mut output = Vec::new();
    FastRsync
        .patch(
            &mut Cursor::new(&old),
            old.len() as u64,
            &mut &delta[..],
            &mut output,
            &context,
        )
        .unwrap();
    assert_eq!(output, new);
}

proptest::proptest! {
    #[test]
    fn roundtrips(old in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096),
                  new in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..4096)) {
        roundtrip(old, new, 16);
    }
}

#[test]
fn lengths_and_repeated_data() {
    for hash in [2, 8, 16] {
        roundtrip(vec![3; 8192], vec![3; 8193], hash);
    }
}

#[test]
fn rejects_invalid_signature_before_library_indexing() {
    let cancel = CancellationToken::new();
    let ctx = DeltaContext::new(&cancel, 1024 * 1024, 1024);
    let sig = [0x72, 0x73, 0x01, 0x36, 0, 0, 0, 0, 0, 0, 0, 16];
    assert!(matches!(
        FastRsync.delta(
            &mut &sig[..],
            &mut Cursor::new(b"x"),
            1,
            &mut Vec::new(),
            &ctx
        ),
        Err(DeltaError::InvalidSignature)
    ));
    assert!(
        FastRsync
            .validate_options(&SignatureOptions::new(0, 16))
            .is_err()
    );
    assert!(
        FastRsync
            .validate_options(&SignatureOptions::new(64, 17))
            .is_err()
    );
}

#[test]
fn limits_and_cancel() {
    let cancel = CancellationToken::new();
    let ctx = DeltaContext::new(&cancel, 64, 16);
    assert!(
        FastRsync
            .signature(
                &mut Cursor::new(vec![0; 100]),
                100,
                &SignatureOptions::new(64, 16),
                &mut Vec::new(),
                &ctx
            )
            .is_err()
    );
    cancel.cancel();
    assert!(matches!(
        FastRsync.signature(
            &mut Cursor::new(b""),
            0,
            &SignatureOptions::new(64, 16),
            &mut Vec::new(),
            &ctx
        ),
        Err(DeltaError::Cancelled)
    ));
}
