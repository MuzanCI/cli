use muzanci_transport::message::AccountId;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_config::config::DebugClientConfig;
use muzanci_config::config::DebugSessionId;
use muzanci_observability::TError;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::DebugResolverMessage;
use muzanci_transport::message::Message;
use muzanci_transport::mux::MuxHandle;

pub struct DebugResolver {
    client_shutdown: CancellationToken,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
}

impl DebugResolver {
    pub fn spawn(
        client_shutdown: CancellationToken,
        mux_handle: MuxHandle,
        user_id: Uuid,
        account_id: AccountId,
        capacity: u64,
    ) -> JoinHandle<Result<DebugClientConfig, TError>> {
        tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) =
                mux_handle.open_channel(ChannelType::DebugResolver).await?;

            DebugResolver {
                client_shutdown,
                channel_tx,
                channel_rx,
            }
            .main(channel_closed, user_id, account_id, capacity)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "debug_resolver",
        fields(
            id = Uuid::now_v7().to_string(),
            user_id = user_id.to_string(),
            account_id = account_id.to_string(),
        )
    )]
    async fn main(
        mut self,
        channel_closed: CancellationToken,
        user_id: Uuid,
        account_id: AccountId,
        capacity: u64,
    ) -> Result<DebugClientConfig, TError> {
        let client_shutdown = self.client_shutdown.clone();
        tokio::select! {
            biased;
            _ = client_shutdown.cancelled() => {
                Err(TError::new("DebugResolver client shutdown before completion."))
            }

            _ = channel_closed.cancelled() => {
                Err(TError::new("DebugResolver channel closed before completion."))
            }

            result = self.run(capacity) => {
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
    async fn run(&mut self, capacity: u64) -> Result<DebugClientConfig, TError> {
        let debug_session_id = self.create_debug_session(capacity).await?;
        eprintln!("Waiting for available runner...");
        let config = self.resolve_debug_client_config(debug_session_id).await?;
        eprintln!("Runner found.");
        Ok(config)
    }

    #[tracing::instrument(skip_all)]
    async fn create_debug_session(&mut self, capacity: u64) -> Result<DebugSessionId, TError> {
        self.channel_tx
            .send(Message::DebugResolver(
                DebugResolverMessage::CreateDebugSessionRequest { capacity },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugResolver(DebugResolverMessage::CreateDebugSessionResponse {
                    result,
                }) => result.map_err(|e| TError::new(&e.to_string())),
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn resolve_debug_client_config(
        &mut self,
        debug_session_id: DebugSessionId,
    ) -> Result<DebugClientConfig, TError> {
        self.channel_tx
            .send(Message::DebugResolver(
                DebugResolverMessage::ResolveDebugClientConfigRequest { debug_session_id },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugResolver(
                    DebugResolverMessage::ResolveDebugClientConfigResponse { result },
                ) => result.map_err(|e| TError::new(&e.to_string())),
                _ => Err(TError::new("Unexpected message type")),
            })
    }
}
