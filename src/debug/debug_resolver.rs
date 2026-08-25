use muzanci_config::config::DebugSessionConfig;
use muzanci_transport::message::DebugResolverMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::ServerId;
use tokio_util::sync::CancellationToken;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::mux::MuxHandle;

pub struct DebugResolverHandle {
    handle: tokio::task::JoinHandle<Result<ServerId, anyhow::Error>>,
}

impl Future for DebugResolverHandle {
    type Output = Result<Result<ServerId, anyhow::Error>, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebugResolver {
    mux_handle: MuxHandle,
    cancellation_token: CancellationToken,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    debug_session_config: DebugSessionConfig,
}

impl DebugResolver {
    pub fn spawn(
        mux_handle: MuxHandle,
        cancellation_token: CancellationToken,
        debug_session_config: DebugSessionConfig,
    ) -> DebugResolverHandle {
        let handle = tokio::spawn(async move {
            tracing::info!("opening debug client channel");
            let (channel_tx, channel_rx) = mux_handle
                .open_channel(ChannelType::DebugResolver)
                .await
                .unwrap();
            tracing::info!("running debug client actor");
            DebugResolver {
                mux_handle,
                cancellation_token,
                channel_tx,
                channel_rx,
                debug_session_config,
            }
            .run()
            .await
        });
        DebugResolverHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self) -> anyhow::Result<ServerId> {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("DebugResolver received cancellation signal.");
                Err(anyhow::anyhow!("Cancelled"))
            }

            result = self.main() => {
                match result {
                    Ok(server_id) => {
                        tracing::info!("DebugResolver finished running.");
                        Ok(server_id)
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
    async fn main(&mut self) -> anyhow::Result<ServerId> {
        self.create_debug().await?;
        let server_id = self.find_debugger().await?;
        Ok(server_id)
    }

    #[tracing::instrument(skip_all)]
    async fn create_debug(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugResolver(
                DebugResolverMessage::CreateDebugSessionRequest {
                    debug_session_config: self.debug_session_config.clone(),
                },
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
    async fn find_debugger(&mut self) -> anyhow::Result<ServerId> {
        self.channel_tx
            .send(Message::DebugResolver(
                DebugResolverMessage::FindDebuggerRequest {
                    debug_session_id: self.debug_session_config.debug_session_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugResolver(DebugResolverMessage::FindDebuggerResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }
}
