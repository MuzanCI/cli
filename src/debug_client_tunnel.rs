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
use crate::stdin::StdinStream;

#[tracing::instrument(skip_all)]
pub async fn run_debug_client_tunnel(
    stdin: &mut StdinStream,
    mux_handle: MuxHandle,
    cancellation_token: CancellationToken,
    debug_id: DebugId,
) -> anyhow::Result<()> {
    let (channel_tx, channel_rx) = mux_handle
        .open_channel(ChannelType::DebugClientTunnel)
        .await?;

    tokio::select! {
        _ = cancellation_token.cancelled() => {
            tracing::info!("DebugClientTunnel received cancellation signal.");
            Ok(())
        }

        result = run(stdin, channel_tx, channel_rx, debug_id) => {
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
async fn run(
    stdin: &mut StdinStream,
    channel_tx: ChannelSender,
    mut channel_rx: ChannelReceiver,
    debug_id: DebugId,
) -> anyhow::Result<()> {
    connect_debug_tunnel(&channel_tx, &mut channel_rx, debug_id).await?;
    tracing::info!("Connected debug tunnel");
    let stream = combine_into_byte_stream(channel_tx, channel_rx);
    let _session = start_ssh_session(stdin, stream).await?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn connect_debug_tunnel(
    channel_tx: &ChannelSender,
    channel_rx: &mut ChannelReceiver,
    debug_id: DebugId,
) -> anyhow::Result<()> {
    channel_tx
        .send(Message::DebugClientTunnel(
            DebugClientTunnelMessage::ConnectDebugTunnelRequest { debug_id },
        ))
        .await?;

    channel_rx
        .recv()
        .await
        .ok_or(anyhow::anyhow!("Channel closed"))
        .and_then(|response| match response {
            Message::DebugClientTunnel(DebugClientTunnelMessage::ConnectDebugTunnelResponse {
                result,
            }) => result.map_err(|e| anyhow::anyhow!(e)),
            _ => Err(anyhow::anyhow!("Unexpected message type")),
        })?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn start_ssh_session(
    stdin: &mut StdinStream,
    stream: ChannelByteStream,
) -> anyhow::Result<()> {
    let config = Arc::new(russh::client::Config::default());
    let client_handler = ClientHandler;
    let mut session_handle = russh::client::connect_stream(config, stream, client_handler).await?;
    tracing::info!("Connected to server");
    let auth_result = session_handle
        .authenticate_none("muzanci-debug-client-tunnel")
        .await?;
    tracing::info!("Authentication result: {:?}", auth_result);
    if !auth_result.success() {
        anyhow::bail!("Authentication failed");
    }
    match run_interactive_shell(stdin, &mut session_handle).await {
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

    tracing::info!("Session closed");
    Ok(())
}
