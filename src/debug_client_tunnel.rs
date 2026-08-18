use std::sync::Arc;

use muzanci_transport::channel::ChannelByteStream;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::combine_into_byte_stream;
use muzanci_transport::message::DebugClientTunnelMessage;
use muzanci_transport::message::Message;
use tokio_util::sync::CancellationToken;

use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::DebugId;
use muzanci_transport::mux::MuxHandle;

use crate::ssh::client::ClientHandler;
use crate::ssh::client::run_interactive_shell;

pub struct DebugClientTunnelHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebugClientTunnelHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebugClientTunnel {
    cancellation_token: CancellationToken,
    debug_id: DebugId,
    channel_tx: Option<ChannelSender>,
    channel_rx: Option<ChannelReceiver>,
}

impl DebugClientTunnel {
    pub fn spawn(
        mux_handle: MuxHandle,
        cancellation_token: CancellationToken,
        debug_id: DebugId,
    ) -> DebugClientTunnelHandle {
        let handle = tokio::spawn(async move {
            tracing::info!("opening debug client channel");
            let (channel_tx, channel_rx) = mux_handle
                .open_channel(ChannelType::DebugClientTunnel)
                .await
                .unwrap();
            DebugClientTunnel {
                cancellation_token,
                debug_id,
                channel_tx: Some(channel_tx),
                channel_rx: Some(channel_rx),
            }
            .run()
            .await
            .unwrap();
        });
        DebugClientTunnelHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("DebugClientTunnel received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                match result {
                    Ok(_) => {
                        tracing::info!("DebugClientTunnel finished running.");
                    }
                    Err(e) => {
                        tracing::error!("DebugClientTunnel encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn main(&mut self) -> anyhow::Result<()> {
        self.connect_debug_tunnel().await?;
        tracing::info!("Connected debug tunnel");
        let _session = self.start_ssh_session().await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn connect_debug_tunnel(&mut self) -> anyhow::Result<()> {
        let channel_tx = self
            .channel_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("channel_tx is not set"))?;

        let channel_rx = self
            .channel_rx
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("channel_rx is not set"))?;

        channel_tx
            .send(Message::DebugClientTunnel(
                DebugClientTunnelMessage::ConnectDebugTunnelRequest {
                    debug_id: self.debug_id,
                },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClientTunnel(
                    DebugClientTunnelMessage::ConnectDebugTunnelResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn start_ssh_session(&mut self) -> anyhow::Result<()> {
        let config = Arc::new(russh::client::Config::default());

        let stream = {
            let channel_tx = self
                .channel_tx
                .take()
                .ok_or_else(|| anyhow::anyhow!("channel_tx is not set"))?;
            let channel_rx = self
                .channel_rx
                .take()
                .ok_or_else(|| anyhow::anyhow!("channel_rx is not set"))?;
            combine_into_byte_stream(channel_tx, channel_rx)
        };
        let client_handler = ClientHandler;
        let mut session_handle =
            russh::client::connect_stream(config, stream, client_handler).await?;
        tracing::info!("Connected to server");
        let auth_result = session_handle
            .authenticate_none("muzanci-debug-client-tunnel")
            .await?;
        tracing::info!("Authentication result: {:?}", auth_result);
        if !auth_result.success() {
            anyhow::bail!("Authentication failed");
        }
        match run_interactive_shell(&mut session_handle).await {
            Ok(exit_code) => {
                tracing::info!("Shell exited with code: {}", exit_code);
            }
            Err(e) => {
                anyhow::bail!("Shell error: {}", e);
            }
        }

        let _ = session_handle
            .disconnect(russh::Disconnect::ByApplication, "Session ended", "en")
            .await;

        session_handle.await?;
        tracing::info!("Session closed");
        Ok(())
    }
}
