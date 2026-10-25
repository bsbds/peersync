//! Shutdown for the CLI's dedicated, one-operation connection.
use anyhow::{Context, Result, bail};
use iroh::endpoint::{Connection, ConnectionError, SendStream};
use peersync::CancellationToken;
use std::time::Duration;

pub(super) enum Role {
    Sender,
    Receiver,
}

/// Call only after the sync protocol succeeds on this role.
/// The receiver has read the sender's final Finish message at that point.
/// Only it closes the connection: symmetric finish/stopped/close can lose the
/// other side's last transport ACK and turn successful syncs into failures.
pub(super) async fn complete(
    connection: &Connection,
    send: &mut SendStream,
    role: Role,
    cancel: &CancellationToken,
) -> Result<()> {
    let completion = async {
        match role {
            Role::Sender => {
                // Do not require our FIN to be acknowledged: the receiver may
                // already have closed after reading our application Finish.
                // Keep both stream halves alive while awaiting its confirmation.
                match connection.closed().await {
                    ConnectionError::ApplicationClosed(close)
                        if close.error_code == 0u32.into() =>
                    {
                        Ok(())
                    }
                    error => bail!("receiver did not confirm completion: {error}"),
                }
            }
            Role::Receiver => {
                send.finish().context("finish receiver stream")?;
                if let Some(code) = send.stopped().await.context("deliver receiver stream")? {
                    bail!("sender stopped receiver stream: {code}");
                }
                connection.close(0u32.into(), b"complete");
                Ok(())
            }
        }
    };
    tokio::select! {
        biased;
        () = cancel.cancelled() => bail!("cancelled while confirming completion"),
        result = tokio::time::timeout(Duration::from_secs(5), completion) => {
            result.context("timed out confirming completion")?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{Endpoint, endpoint::presets};

    async fn pair() -> (
        Endpoint,
        Endpoint,
        Connection,
        Connection,
        SendStream,
        SendStream,
    ) {
        let a = Endpoint::builder(presets::Minimal)
            .alpns(vec![crate::ALPN.to_vec()])
            .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .unwrap()
            .bind()
            .await
            .unwrap();
        let b = Endpoint::builder(presets::Minimal)
            .alpns(vec![crate::ALPN.to_vec()])
            .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .unwrap()
            .bind()
            .await
            .unwrap();
        let (sender, receiver) = tokio::join!(a.connect(b.addr(), crate::ALPN), async {
            b.accept().await.unwrap().await
        });
        let sender = sender.unwrap();
        let receiver = receiver.unwrap();
        let (mut tx, rx) = sender.open_bi().await.unwrap();
        tx.write_all(b"finish").await.unwrap();
        let (reply, mut incoming) = receiver.accept_bi().await.unwrap();
        let mut bytes = [0; 6];
        incoming.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"finish");
        // Keep read halves alive until the connections close, as run() does.
        let watched = sender.clone();
        tokio::spawn(async move {
            watched.closed().await;
            drop(rx);
            drop(incoming);
        });
        (a, b, sender, receiver, tx, reply)
    }

    #[tokio::test]
    async fn sender_waits_for_receiver_and_accepts_already_delivered_close() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (a, b, sender, receiver, mut tx, mut reply) = pair().await;
            let cancel = CancellationToken::new();
            // A local transport ACK is not permission for the sender to close.
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(30),
                    complete(&sender, &mut tx, Role::Sender, &cancel)
                )
                .await
                .is_err()
            );
            assert!(receiver.close_reason().is_none());
            complete(&receiver, &mut reply, Role::Receiver, &cancel)
                .await
                .unwrap();
            sender.closed().await;
            // Regression: receiver close may arrive before sender completion is polled.
            complete(&sender, &mut tx, Role::Sender, &cancel)
                .await
                .unwrap();
            a.close().await;
            b.close().await;
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn abnormal_close_and_cancellation_are_not_success() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let (a, b, sender, receiver, mut tx, _reply) = pair().await;
            let cancel = CancellationToken::new();
            cancel.cancel();
            assert!(
                complete(&sender, &mut tx, Role::Sender, &cancel)
                    .await
                    .is_err()
            );
            receiver.close(1u32.into(), b"failed");
            assert!(
                complete(&sender, &mut tx, Role::Sender, &CancellationToken::new())
                    .await
                    .is_err()
            );
            a.close().await;
            b.close().await;
        })
        .await
        .unwrap();
    }
}
