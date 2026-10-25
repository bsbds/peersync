//! Replaceable synchronous file-delta engine. Network transport and retry policy
//! deliberately live outside this module. Inputs must be bounded before indexing.
use std::io::{self, Read, Seek, Write};
use tokio_util::sync::CancellationToken;

pub trait ReadSeek: Read + Seek {}
impl<T: Read + Seek + ?Sized> ReadSeek for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DeltaFormat {
    pub signature_id: &'static str,
    pub delta_id: &'static str,
    pub version: u16,
}
impl DeltaFormat {
    pub fn new(signature_id: &'static str, delta_id: &'static str, version: u16) -> Self {
        Self {
            signature_id,
            delta_id,
            version,
        }
    }
}
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct SignatureOptions {
    pub block_size: u32,
    pub hash_bytes: u8,
}
impl SignatureOptions {
    pub fn new(block_size: u32, hash_bytes: u8) -> Self {
        Self {
            block_size,
            hash_bytes,
        }
    }
}
#[non_exhaustive]
pub struct DeltaContext<'a> {
    pub cancel: &'a CancellationToken,
    pub max_working_bytes: usize,
    pub output_limit: u64,
}
impl<'a> DeltaContext<'a> {
    pub fn new(cancel: &'a CancellationToken, max_working_bytes: usize, output_limit: u64) -> Self {
        Self {
            cancel,
            max_working_bytes,
            output_limit,
        }
    }
    fn check(&self) -> Result<(), DeltaError> {
        if self.cancel.is_cancelled() {
            Err(DeltaError::Cancelled)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum DeltaError {
    #[error("invalid signature or parameters")]
    InvalidSignature,
    #[error("delta resource budget exceeded")]
    Limit,
    #[error("delta operation cancelled")]
    Cancelled,
    #[error("delta I/O: {0}")]
    Io(#[from] io::Error),
    #[error("invalid delta: {0}")]
    InvalidDelta(String),
}

pub trait DeltaEngine: Send + Sync {
    fn format(&self) -> DeltaFormat;
    fn validate_options(&self, options: &SignatureOptions) -> Result<(), DeltaError>;
    fn signature(
        &self,
        basis: &mut dyn ReadSeek,
        basis_len: u64,
        options: &SignatureOptions,
        output: &mut dyn Write,
        context: &DeltaContext<'_>,
    ) -> Result<(), DeltaError>;
    fn delta(
        &self,
        signature: &mut dyn Read,
        source: &mut dyn ReadSeek,
        source_len: u64,
        output: &mut dyn Write,
        context: &DeltaContext<'_>,
    ) -> Result<(), DeltaError>;
    fn patch(
        &self,
        basis: &mut dyn ReadSeek,
        basis_len: u64,
        delta: &mut dyn Read,
        output: &mut dyn Write,
        context: &DeltaContext<'_>,
    ) -> Result<(), DeltaError>;
}

#[derive(Debug, Default)]
pub struct FastRsync;

fn bounded_read(input: &mut dyn Read, limit: usize) -> Result<Vec<u8>, DeltaError> {
    let mut bytes = Vec::new();
    input.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err(DeltaError::Limit);
    }
    Ok(bytes)
}
fn exact_read(input: &mut dyn ReadSeek, len: u64, limit: usize) -> Result<Vec<u8>, DeltaError> {
    if len > limit as u64 {
        return Err(DeltaError::Limit);
    }
    let bytes = bounded_read(&mut input.take(len + 1), len as usize)?;
    if bytes.len() as u64 != len {
        return Err(DeltaError::InvalidDelta("input length changed".into()));
    }
    Ok(bytes)
}
struct LimitedWriter<'a> {
    output: &'a mut dyn Write,
    remaining: u64,
    cancel: &'a CancellationToken,
}
impl Write for LimitedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancel.is_cancelled() {
            return Err(io::ErrorKind::Interrupted.into());
        }
        if bytes.len() as u64 > self.remaining {
            return Err(io::Error::other("output budget exceeded"));
        }
        let n = self.output.write(bytes)?;
        self.remaining -= n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.output.flush()
    }
}
impl DeltaEngine for FastRsync {
    fn format(&self) -> DeltaFormat {
        DeltaFormat::new("librsync-md4", "librsync-delta", 1)
    }
    fn validate_options(&self, options: &SignatureOptions) -> Result<(), DeltaError> {
        if options.block_size == 0 || !(1..=16).contains(&options.hash_bytes) {
            Err(DeltaError::InvalidSignature)
        } else {
            Ok(())
        }
    }
    fn signature(
        &self,
        basis: &mut dyn ReadSeek,
        len: u64,
        options: &SignatureOptions,
        output: &mut dyn Write,
        context: &DeltaContext<'_>,
    ) -> Result<(), DeltaError> {
        context.check()?;
        self.validate_options(options)?;
        let budget = context.max_working_bytes / 4;
        let data = exact_read(basis, len, budget)?;
        let size = 12u64.saturating_add(
            len.div_ceil(options.block_size as u64)
                .saturating_mul(4 + options.hash_bytes as u64),
        );
        if size > context.output_limit || size > budget as u64 {
            return Err(DeltaError::Limit);
        }
        let sig = fast_rsync::Signature::calculate(
            &data,
            fast_rsync::SignatureOptions {
                block_size: options.block_size,
                crypto_hash_size: options.hash_bytes as u32,
            },
        );
        context.check()?;
        output.write_all(sig.serialized())?;
        Ok(())
    }
    fn delta(
        &self,
        signature: &mut dyn Read,
        source: &mut dyn ReadSeek,
        len: u64,
        output: &mut dyn Write,
        context: &DeltaContext<'_>,
    ) -> Result<(), DeltaError> {
        context.check()?;
        let budget = context.max_working_bytes / 8;
        let sig = bounded_read(signature, budget)?;
        if sig.len() < 12 {
            return Err(DeltaError::InvalidSignature);
        }
        let block_size = u32::from_be_bytes(sig[4..8].try_into().unwrap());
        let hash = u32::from_be_bytes(sig[8..12].try_into().unwrap());
        if sig[..4] != [0x72, 0x73, 0x01, 0x36] || hash == 0 || hash > 16 || block_size == 0 {
            return Err(DeltaError::InvalidSignature);
        }
        let stride = 4 + hash as usize;
        if !(sig.len() - 12).is_multiple_of(stride) {
            return Err(DeltaError::InvalidSignature);
        }
        // Conservative index reservation: bound attacker-controlled block count
        // before fast_rsync allocates its nested HashMaps.
        if (sig.len() - 12) / stride > budget / 256 {
            return Err(DeltaError::Limit);
        }
        let sig =
            fast_rsync::Signature::deserialize(sig).map_err(|_| DeltaError::InvalidSignature)?;
        let data = exact_read(source, len, budget)?;
        let out = LimitedWriter {
            output,
            remaining: context.output_limit,
            cancel: context.cancel,
        };
        fast_rsync::diff(&sig.index(), &data, out)
            .map_err(|e| DeltaError::InvalidDelta(e.to_string()))?;
        context.check()
    }
    fn patch(
        &self,
        basis: &mut dyn ReadSeek,
        len: u64,
        delta: &mut dyn Read,
        output: &mut dyn Write,
        context: &DeltaContext<'_>,
    ) -> Result<(), DeltaError> {
        context.check()?;
        let budget = context.max_working_bytes / 4;
        let base = exact_read(basis, len, budget)?;
        let delta = bounded_read(delta, budget)?;
        let mut out = LimitedWriter {
            output,
            remaining: context.output_limit,
            cancel: context.cancel,
        };
        let limit = usize::try_from(context.output_limit).map_err(|_| DeltaError::Limit)?;
        fast_rsync::apply_limited(&base, &delta, &mut out, limit)
            .map_err(|e| DeltaError::InvalidDelta(e.to_string()))?;
        context.check()
    }
}

/// Rsync 3.4.1-style block length and checksum length heuristic (protocol >=30).
/// This selects parameters, not rsync wire compatibility or identical matching.
pub(crate) fn signature_options(len: u64, redo: bool) -> SignatureOptions {
    let mut block = 700u64;
    if len > 700 * 700 {
        let mut lo = 0u64;
        let mut hi = len.min(131072);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if mid <= len / mid {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        block = (lo / 8 * 8).max(700);
    }
    let log = |x: u64| {
        if x == 0 {
            0
        } else {
            63 - x.leading_zeros() as i32
        }
    };
    let bits = 10 + 2 * log(len) - log(block);
    let hash_bytes = if redo {
        16
    } else {
        ((bits + 1 - 32 + 7) / 8).clamp(2, 16) as u8
    };
    SignatureOptions::new(block as u32, hash_bytes)
}
