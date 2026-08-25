use std::path::PathBuf;

use http::Request;
use muzanci_transport::message::ServerId;
use tokio_util::sync::CancellationToken;

use muzanci_config::JobConfig;
use muzanci_config::config::DebugSessionConfig;
use muzanci_config::config::DebugSessionId;
use muzanci_git::GitClient;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;

use crate::debug::debug_client::DebugClient;
use crate::debug::debug_resolver::DebugResolver;

pub fn run_debug_session(job: JobConfig) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        let cancellation_token = CancellationToken::new();

        let debug_session_id = DebugSessionId::now_v7();

        let remote = {
            // TODO: Refactor these to be CLI options.
            let target_dir = PathBuf::from("./.git");
            let remote_name = "origin";
            GitClient::try_default()?.get_remote(&target_dir, remote_name)?
        };

        let hostname = "localhost:8002";

        let server_id = {
            let capacity = 1;
            let debug_session_config = DebugSessionConfig {
                debug_session_id,
                capacity,
            };
            let mux_handle = connect_debug_resolver(hostname, cancellation_token.clone()).await?;
            let debug_resolver_handle =
                DebugResolver::spawn(mux_handle, cancellation_token.clone(), debug_session_config);
            debug_resolver_handle.await??
        };

        let debug_client_handle = {
            let mux_handle =
                connect_debug_client(hostname, cancellation_token.clone(), server_id).await?;
            DebugClient::spawn(
                mux_handle,
                cancellation_token,
                debug_session_id,
                remote,
                job,
            )
        };
        debug_client_handle.await?;

        tracing::info!("debug session ended");

        // DebugClient::debugger_control spawns a StdinStream task that blocks on a read syscall and
        //  will not return until stdin is flushed with a newline. To avoid blocking process exit,
        //  we explicitly exit immediately after the debug session ends.
        std::process::exit(0);
    })
}

#[tracing::instrument(skip_all)]
pub async fn connect_debug_resolver(
    hostname: &str,
    cancellation_token: CancellationToken,
) -> anyhow::Result<MuxHandle> {
    let server_stream = {
        let stream = tokio::net::TcpStream::connect(hostname).await?;
        stream.set_nodelay(true)?;
        hyper_util::rt::TokioIo::new(stream)
    };

    let (mut send_request, connection) =
        hyper::client::conn::http1::handshake(server_stream).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.with_upgrades().await {
            eprintln!("Connection error: {:?}", e);
        }
    });

    let request = Request::builder()
        .method("POST")
        .uri("/debug_resolver")
        .header(http::header::HOST, hostname)
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::UPGRADE, MUZANCI_TRANSPORT_V1)
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();

    let response = send_request.send_request(request).await?;

    if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(anyhow::anyhow!(
            "Failed to upgrade connection. Server responded with status: {}",
            response.status()
        ));
    }

    let server_stream = hyper::upgrade::on(response).await?;
    let server_stream = hyper_util::rt::TokioIo::new(server_stream);

    let channel_acceptor = FnChannelAcceptor::new(move |channel_id, channel_type| {
        panic!(
            "Client received request to open channel [{}] of type {:?}",
            channel_id, channel_type
        );
    });

    let mux_handle = Mux::spawn(server_stream, channel_acceptor, cancellation_token);

    Ok(mux_handle)
}

#[tracing::instrument(skip_all)]
pub async fn connect_debug_client(
    hostname: &str,
    cancellation_token: CancellationToken,
    server_id: ServerId,
) -> anyhow::Result<MuxHandle> {
    let server_stream = {
        let stream = tokio::net::TcpStream::connect(hostname).await?;
        stream.set_nodelay(true)?;
        hyper_util::rt::TokioIo::new(stream)
    };

    let (mut send_request, connection) =
        hyper::client::conn::http1::handshake(server_stream).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.with_upgrades().await {
            eprintln!("Connection error: {:?}", e);
        }
    });

    let request = Request::builder()
        .method("POST")
        .uri("/debug_client")
        .header(http::header::HOST, hostname)
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::UPGRADE, MUZANCI_TRANSPORT_V1)
        .header("X-MUZANCI-SERVER-ID", server_id.to_string())
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();

    let response = send_request.send_request(request).await?;

    if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(anyhow::anyhow!(
            "Failed to upgrade connection. Server responded with status: {}",
            response.status()
        ));
    }

    let server_stream = hyper::upgrade::on(response).await?;
    let server_stream = hyper_util::rt::TokioIo::new(server_stream);

    let channel_acceptor = FnChannelAcceptor::new(move |channel_id, channel_type| {
        panic!(
            "Client received request to open channel [{}] of type {:?}",
            channel_id, channel_type
        );
    });

    let mux_handle = Mux::spawn(server_stream, channel_acceptor, cancellation_token);

    Ok(mux_handle)
}
