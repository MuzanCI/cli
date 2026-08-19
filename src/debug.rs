use std::path::PathBuf;

use http::Request;
use tokio_util::sync::CancellationToken;

use muzanci_git::GitClient;
use muzanci_interpreter::JobConfig;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::message::DebugConfig;
use muzanci_transport::message::DebugId;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;

use crate::debug_client::DebugClient;

pub fn run_debug_session(job: JobConfig) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        let cancellation_token = CancellationToken::new();

        let remote = {
            // TODO: Refactor these to be CLI options.
            let target_dir = PathBuf::from("./.git");
            let remote_name = "origin";
            GitClient::try_default()?.get_remote(&target_dir, remote_name)?
        };

        let hostname = "localhost:8002";
        let mux_handle = connect(hostname, cancellation_token.clone()).await?;

        let capacity = 1;
        let debug_config = DebugConfig {
            debug_id: DebugId::now_v7(),
            job,
            remote,
            capacity,
        };
        let debug_client_handle = DebugClient::spawn(mux_handle, cancellation_token, debug_config);
        debug_client_handle.await?;

        tracing::info!("debug session ended");

        // DebugClient::debugger_control spawns a StdinStream task that blocks on a read syscall and
        //  will not return until stdin is flushed with a newline. To avoid blocking process exit,
        //  we explicitly exit immediately after the debug session ends.
        std::process::exit(0);
    })
}

#[tracing::instrument(skip_all)]
pub async fn connect(
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
        .uri("/debug")
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
