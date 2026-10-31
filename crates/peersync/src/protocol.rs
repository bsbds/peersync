use crate::{Limits, Resource, SyncError, SyncOptions};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;

pub(crate) const VERSION: u16 = 2;
const CONTROL: u8 = 1;
const DATA: u8 = 2;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub(crate) enum Message {
    Hello {
        version: u16,
        signature: String,
        delta: String,
        format_version: u16,
        options: SyncOptions,
    },
    Ready,
    Entry(crate::fs::Entry),
    FilterQuery {
        candidates: Vec<crate::filter::Candidate>,
    },
    FilterResult {
        included: Vec<bool>,
    },
    ScanIssue {
        path: Option<crate::fs::WirePath>,
        message: String,
    },
    ManifestEnd,
    Request {
        index: usize,
        hash_bytes: u8,
        signature_len: u64,
    },
    Content {
        bytes: u64,
        file_len: u64,
        digest: String,
        delta: bool,
    },
    ContentEnd {
        digest: String,
    },
    Failed {
        message: String,
    },
    ItemResult {
        committed: bool,
    },
    Done {
        failed: u64,
        committed: u64,
        planned: u64,
        skipped: u64,
    },
    Finish,
}

pub(crate) fn invalid(message: impl Into<String>) -> SyncError {
    SyncError::Protocol {
        message: message.into(),
    }
}

/// Count protocol bytes, not transport overhead. Dropping/cancelling an in-flight
/// frame invalidates this stream; partial framing is never resumed.
pub(crate) struct Wire<'a, R, W> {
    pub reader: &'a mut R,
    pub writer: &'a mut W,
    pub limits: &'a Limits,
    pub cancel: CancellationToken,
    pub sent: u64,
    pub received: u64,
}

impl<R: AsyncRead + Unpin + Send, W: AsyncWrite + Unpin + Send> Wire<'_, R, W> {
    pub async fn send(&mut self, message: &Message) -> Result<(), SyncError> {
        let data = serde_json::to_vec(message).map_err(|e| invalid(e.to_string()))?;
        self.frame(CONTROL, &data).await
    }
    pub async fn receive(&mut self) -> Result<Message, SyncError> {
        let bytes = self.read_frame(CONTROL).await?;
        decode(&bytes)
    }
    pub async fn send_data(&mut self, data: &[u8]) -> Result<(), SyncError> {
        for chunk in data.chunks(self.limits.max_data_frame_bytes) {
            self.frame(DATA, chunk).await?;
        }
        Ok(())
    }
    pub async fn receive_data(&mut self, len: u64, max: usize) -> Result<Vec<u8>, SyncError> {
        if len > max as u64 {
            return Err(SyncError::LimitExceeded {
                resource: Resource::BufferMemory,
            });
        }
        let mut output = Vec::with_capacity(len as usize);
        while output.len() < len as usize {
            let part = self.read_frame(DATA).await?;
            if part.is_empty() || part.len() > len as usize - output.len() {
                return Err(invalid("invalid data length"));
            }
            output.extend_from_slice(&part);
        }
        Ok(output)
    }
    async fn frame(&mut self, kind: u8, bytes: &[u8]) -> Result<(), SyncError> {
        let limit = if kind == CONTROL {
            self.limits.max_control_frame_bytes
        } else {
            self.limits.max_data_frame_bytes
        };
        if bytes.len() > limit {
            return Err(SyncError::LimitExceeded {
                resource: if kind == CONTROL {
                    Resource::ControlFrame
                } else {
                    Resource::DataFrame
                },
            });
        }
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(SyncError::Cancelled),
            result = async {
                self.writer.write_u8(kind).await?;
                self.writer.write_u32(bytes.len() as u32).await?;
                self.writer.write_all(bytes).await?;
                self.writer.flush().await
            } => { result?; self.sent += 5 + bytes.len() as u64; Ok(()) }
        }
    }
    pub async fn read_data_chunk(&mut self) -> Result<Vec<u8>, SyncError> {
        self.read_frame(DATA).await
    }
    async fn read_frame(&mut self, expected: u8) -> Result<Vec<u8>, SyncError> {
        tokio::select! {
            biased;
            () = self.cancel.cancelled() => Err(SyncError::Cancelled),
            result = async {
                let kind = self.reader.read_u8().await?;
                let len = self.reader.read_u32().await? as usize;
                if kind != expected { return Err(invalid("unexpected frame type")); }
                let limit = if kind == CONTROL { self.limits.max_control_frame_bytes } else { self.limits.max_data_frame_bytes };
                if len > limit { return Err(SyncError::LimitExceeded { resource: if kind == CONTROL { Resource::ControlFrame } else { Resource::DataFrame } }); }
                let mut bytes = vec![0; len];
                self.reader.read_exact(&mut bytes).await?;
                self.received += 5 + len as u64;
                Ok(bytes)
            } => result,
        }
    }
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, SyncError> {
    serde_json::from_slice(bytes).map_err(|e| invalid(format!("invalid control frame: {e}")))
}
