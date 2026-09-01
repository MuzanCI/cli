use muzanci_config::config::DebugClientConfig;
use muzanci_config::config::DebugSessionId;
use muzanci_transport::message::DebugResolverMessage;
use muzanci_transport::message::Message;
use tokio_util::sync::CancellationToken;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::mux::MuxHandle;

pub struct DebugResolverHandle {
    handle: tokio::task::JoinHandle<Result<DebugClientConfig, anyhow::Error>>,
}

impl Future for DebugResolverHandle {
    type Output = Result<Result<DebugClientConfig, anyhow::Error>, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebugResolver {
    cancellation_token: CancellationToken,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
}

impl DebugResolver {
    pub fn spawn(
        mux_handle: MuxHandle,
        cancellation_token: CancellationToken,
        capacity: u64,
    ) -> DebugResolverHandle {
        let handle = tokio::spawn(async move {
            tracing::info!("opening debug client channel");
            let (channel_tx, channel_rx, _notify) = mux_handle
                .open_channel(ChannelType::DebugResolver)
                .await
                .unwrap();
            tracing::info!("running debug client actor");
            DebugResolver {
                cancellation_token,
                channel_tx,
                channel_rx,
            }
            .run(capacity)
            .await
        });
        DebugResolverHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self, capacity: u64) -> anyhow::Result<DebugClientConfig> {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("DebugResolver received cancellation signal.");
                Err(anyhow::anyhow!("Cancelled"))
            }

            result = self.main(capacity) => {
                match result {
                    Ok(config) => {
                        tracing::info!("DebugResolver finished running.");
                        Ok(config)
                    }
                    Err(e) => {
                        tracing::error!("DebugResolver encountered an error: {:?}", e);
                        Err(e)
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn main(&mut self, capacity: u64) -> anyhow::Result<DebugClientConfig> {
        let debug_session_id = self.create_debug_session(capacity).await?;
        let config = self.resolve_debug_client_config(debug_session_id).await?;
        Ok(config)
    }

    #[tracing::instrument(skip_all)]
    async fn create_debug_session(&mut self, capacity: u64) -> anyhow::Result<DebugSessionId> {
        self.channel_tx
            .send(Message::DebugResolver(
                DebugResolverMessage::CreateDebugSessionRequest { capacity },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugResolver(DebugResolverMessage::CreateDebugSessionResponse {
                    result,
                }) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn resolve_debug_client_config(
        &mut self,
        debug_session_id: DebugSessionId,
    ) -> anyhow::Result<DebugClientConfig> {
        self.channel_tx
            .send(Message::DebugResolver(
                DebugResolverMessage::ResolveDebugClientConfigRequest { debug_session_id },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugResolver(
                    DebugResolverMessage::ResolveDebugClientConfigResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }
}
