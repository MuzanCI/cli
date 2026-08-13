use std::path::PathBuf;

use http::Request;
use muzanci_git::get_remote;
use muzanci_transport::message::DebugClientMessage;
use muzanci_transport::message::DebugConfig;
use muzanci_transport::message::DebugId;
use muzanci_transport::message::Message;
use tokio_util::sync::CancellationToken;

use muzanci_interpreter::JobConfig;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;

pub fn run_debug_session(job: JobConfig) -> anyhow::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async {
        let cancellation_token = CancellationToken::new();

        let hostname = "localhost:8002";
        let mux_handle = connect(hostname, cancellation_token.clone()).await?;

        let remote = {
            // TODO: Refactor these to be CLI options.
            let target_dir = PathBuf::from(".");
            let remote_name = "origin";
            get_remote(&target_dir, remote_name)?
        };

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
        Ok(())
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

pub struct DebugClientHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebugClientHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebugClient {
    mux_handle: MuxHandle,
    cancellation_token: CancellationToken,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    debug_config: DebugConfig,
}

impl DebugClient {
    pub fn spawn(
        mux_handle: MuxHandle,
        cancellation_token: CancellationToken,
        debug_config: DebugConfig,
    ) -> DebugClientHandle {
        let handle = tokio::spawn(async move {
            tracing::info!("opening debug client channel");
            let (channel_tx, channel_rx) = mux_handle
                .open_channel(ChannelType::DebugClient)
                .await
                .unwrap();
            tracing::info!("running debug client actor");
            DebugClient {
                mux_handle,
                cancellation_token,
                channel_tx,
                channel_rx,
                debug_config,
            }
            .run()
            .await
            .unwrap();
        });
        DebugClientHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("DebugClient received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                match result {
                    Ok(_) => {
                        tracing::info!("DebugClient finished running.");
                    }
                    Err(e) => {
                        tracing::error!("DebugClient encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn main(&mut self) -> anyhow::Result<()> {
        self.create_debug().await?;
        self.connect_debugger().await?;
        self.checkout_debugger().await?;
        self.create_diff().await?;
        self.start_diff_upload().await?;
        self.send_diff().await?;
        self.complete_diff_upload().await?;

        for step in self.debug_config.job.steps {
            // Prompt user for
            // "s" to execute step
            // "d" to start shell
            // "q" to quit
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn create_debug(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CreateDebugRequest {
                    debug_config: self.debug_config.clone(),
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::CreateDebugResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn connect_debugger(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ConnectDebuggerRequest,
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::ConnectDebuggerResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn checkout_debugger(&self) -> anyhow::Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn create_diff(&self) -> anyhow::Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn start_diff_upload(&self) -> anyhow::Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn send_diff(&self) -> anyhow::Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn complete_diff_upload(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
