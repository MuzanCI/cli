use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use uuid::Uuid;

use muzanci_config::config::DebugSessionId;
use muzanci_observability::TError;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::channel::combine_into_byte_stream;
use muzanci_transport::message::AccountId;
use muzanci_transport::message::DebugClientTunnelMessage;
use muzanci_transport::message::Message;
use muzanci_transport::mux::MuxHandle;

use crate::ssh_client_handler::ClientHandler;
use crate::ssh_client_handler::run_exec_shell;
use crate::ssh_client_handler::run_interactive_shell;
use crate::stdin::StdinStream;

#[tracing::instrument(
    skip_all,
    name = "debug_client_tunnel_interactive",
    fields(
        id = Uuid::now_v7().to_string().to_string(),
        user_id = user_id.to_string(),
        account_id = account_id.to_string(),
        debug_session_id = debug_session_id.to_string(),
    )
)]
pub async fn tunnel_interactive(
    signal_receiver_enabled: Arc<AtomicBool>,
    stdin: &mut StdinStream,
    mux_handle: MuxHandle,
    user_id: Uuid,
    account_id: AccountId,
    debug_session_id: DebugSessionId,
) -> Result<(), TError> {
    let mut session_handle = init_session_handle(mux_handle, debug_session_id).await?;

    match run_interactive_shell(signal_receiver_enabled, stdin, &mut session_handle).await {
        Ok(exit_code) => {
            tracing::info!("Shell exited with code: {}", exit_code);
        }
        Err(e) => {
            return Err(TError::new(&format!("Shell error: {}", e)));
        }
    }

    session_handle
        .disconnect(russh::Disconnect::ByApplication, "Session ended", "en")
        .await?;

    Ok(())
}

#[tracing::instrument(
    skip_all,
    name = "debug_client_tunnel_interactive",
    fields(
        id = Uuid::now_v7().to_string().to_string(),
        user_id = user_id.to_string(),
        account_id = account_id.to_string(),
        debug_session_id = debug_session_id.to_string(),
    )
)]
pub async fn tunnel_exec(
    signal_receiver_enabled: Arc<AtomicBool>,
    cmd: &str,
    mux_handle: MuxHandle,
    user_id: Uuid,
    account_id: AccountId,
    debug_session_id: DebugSessionId,
) -> Result<u32, TError> {
    let mut session_handle = init_session_handle(mux_handle, debug_session_id).await?;

    let exit_code = match run_exec_shell(signal_receiver_enabled, cmd, &mut session_handle).await {
        Ok(exit_code) => {
            tracing::info!("Shell exited with code: {}", exit_code);
            exit_code
        }
        Err(e) => {
            return Err(TError::new(&format!("Shell error: {}", e)));
        }
    };

    session_handle
        .disconnect(russh::Disconnect::ByApplication, "Session ended", "en")
        .await?;

    Ok(exit_code)
}

#[tracing::instrument(skip_all)]
async fn init_session_handle(
    mux_handle: MuxHandle,
    debug_session_id: DebugSessionId,
) -> Result<russh::client::Handle<ClientHandler>, TError> {
    let (channel_tx, mut channel_rx, _notify) = mux_handle
        .open_channel(ChannelType::DebugClientTunnel)
        .await?;
    connect_debug_tunnel(&channel_tx, &mut channel_rx, debug_session_id).await?;
    let mut session_handle = connect_ssh_session(channel_tx, channel_rx).await?;
    authenticate_ssh_session(&mut session_handle).await?;
    Ok(session_handle)
}

#[tracing::instrument(skip_all)]
async fn connect_debug_tunnel(
    channel_tx: &ChannelSender,
    channel_rx: &mut ChannelReceiver,
    debug_session_id: DebugSessionId,
) -> Result<(), TError> {
    channel_tx
        .send(Message::DebugClientTunnel(
            DebugClientTunnelMessage::ConnectDebugTunnelRequest { debug_session_id },
        ))
        .await?;

    channel_rx
        .recv()
        .await
        .ok_or_else(|| TError::new("Channel closed"))
        .and_then(|response| match response {
            Message::DebugClientTunnel(DebugClientTunnelMessage::ConnectDebugTunnelResponse {
                result,
            }) => result.map_err(|e| TError::new(&e.to_string())),
            _ => Err(TError::new("Unexpected message type")),
        })?;

    Ok(())
}

#[tracing::instrument(skip_all)]
async fn connect_ssh_session(
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
) -> Result<russh::client::Handle<ClientHandler>, TError> {
    let config = Arc::new(russh::client::Config::default());
    let stream = combine_into_byte_stream(channel_tx, channel_rx);
    let client_handler = ClientHandler;
    let session_handle = russh::client::connect_stream(config, stream, client_handler).await?;
    Ok(session_handle)
}

/// No authentication is performed on the SSH session.
///
/// The SSH connection is wrapped in a Mux channel (DebugClientTunnel).
/// The security of the SSH connection depends on the security of the Mux connection,
/// which should be initialized with TCP/TLS.
#[tracing::instrument(skip_all)]
async fn authenticate_ssh_session(
    session_handle: &mut russh::client::Handle<ClientHandler>,
) -> Result<(), TError> {
    let auth_result = session_handle
        .authenticate_none("muzanci-debug-client-tunnel")
        .await?;

    if !auth_result.success() {
        return Err(TError::new("Authentication failed"));
    }

    Ok(())
}
