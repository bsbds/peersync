//! Directory synchronization over caller-owned reliable, ordered byte streams.
//!
//! The caller authenticates and authorizes the peer. This crate does not create
//! connections, provide encryption, or sandbox concurrent local filesystem edits.
//! A failed or cancelled operation can have committed some changes: do not replay
//! it automatically. Cancel cooperatively and await completion for cleanup.
//!
//! ```no_run
//! use peersync::{CancellationToken, SyncContext, SyncOptions, Syncer};
//! use std::path::Path;
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let (transport_a, transport_b) = tokio::io::duplex(64 * 1024);
//! let (mut ar, mut aw) = tokio::io::split(transport_a);
//! let (mut br, mut bw) = tokio::io::split(transport_b);
//! let sender = Syncer::new();
//! let receiver = Syncer::new(); // independent role, independent budget
//! let cancel = CancellationToken::new();
//! let mut send_context = SyncContext::new(cancel.clone());
//! let mut receive_context = SyncContext::new(cancel);
//! let mut options = SyncOptions::default();
//! options.preserve_mtime = true;
//! let (sent, received) = tokio::join!(
//!     sender.send(&mut ar, &mut aw, Path::new("source"), &options, &mut send_context),
//!     receiver.receive(&mut br, &mut bw, Path::new("destination"), Some(&options), &mut receive_context),
//! );
//! sent?;
//! received?;
//! # Ok(()) }
//! ```

pub mod delta;
mod filter;
mod fs;
mod protocol;
mod sync;
mod types;
pub use sync::Syncer;
pub use tokio_util::sync::CancellationToken;
pub use types::*;
