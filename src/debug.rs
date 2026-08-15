use std::path::PathBuf;
use std::process::ExitCode;
use std::process::ExitStatus;

use http::Request;
use muzanci_git::GitClient;
use muzanci_interpreter::StepConfig;
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
        self.debugger_apply_diff().await?;
        self.debugger_control().await?;

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
    async fn checkout_debugger(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CheckoutBranchRequest {
                    url: self.debug_config.remote.url.clone(),
                    branch: self.debug_config.remote.branch.clone(),
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::CheckoutBranchResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn create_diff(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn start_diff_upload(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartDiffUploadRequest,
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::StartDiffUploadResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn send_diff(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::UploadDiffChunkRequest,
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::UploadDiffChunkResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn complete_diff_upload(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CompleteDiffUploadRequest,
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::CompleteDiffUploadResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_apply_diff(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(DebugClientMessage::ApplyDiffRequest))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::ApplyDiffResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_control(&mut self) -> anyhow::Result<()> {
        let mut current_step: usize = 0;
        let mut step_success: Vec<Option<bool>> = vec![None; self.debug_config.job.steps.len()];
        let mut input = String::new();
        loop {
            // print current state
            println!("Job: {}", self.debug_config.job.name);
            println!("Steps:");
            for (i, step) in self.debug_config.job.steps.iter().enumerate() {
                let status = if let Some(success) = step_success[i] {
                    if success { "✔" } else { "✘" }
                } else {
                    "?"
                };
                let cursor = if i == current_step { ">" } else { " " };
                println!(
                    "{} {} ({}) {}\n\t\"{}\"\n",
                    cursor,
                    status,
                    i + 1,
                    step.name,
                    step.command
                );
            }

            println!("You can open a (s)hell, (m)ove, (n)ext, (c)ontinue, or (q)uit.");

            print!("> ");
            use std::io::Write;
            std::io::stdout().flush()?;

            // read user input line
            input.clear();
            std::io::stdin().read_line(&mut input)?;

            // parse into command
            let mut parts = input.trim().split_whitespace();
            let command = parts.next();
            let command = match command {
                Some(command) => match command {
                    "s" | "shell" => {
                        if let Some(i) = parts.next().and_then(|s| s.parse::<usize>().ok()) {
                            DebuggerCommand::Ssh { step_idx: i - 1 }
                        } else {
                            DebuggerCommand::Ssh {
                                step_idx: current_step,
                            }
                        }
                    }
                    "m" | "move" => {
                        if let Some(i) = parts.next().and_then(|s| s.parse::<usize>().ok()) {
                            DebuggerCommand::Move { step_idx: i - 1 }
                        } else {
                            DebuggerCommand::Move {
                                step_idx: current_step,
                            }
                        }
                    }
                    "c" | "continue" => {
                        if let Some(i) = parts.next().and_then(|s| s.parse::<usize>().ok()) {
                            DebuggerCommand::Continue { step_idx: i - 1 }
                        } else {
                            DebuggerCommand::Continue {
                                step_idx: current_step,
                            }
                        }
                    }
                    "n" | "next" => DebuggerCommand::Next,
                    "q" | "quit" => DebuggerCommand::Quit,
                    _ => {
                        println!("Unknown command: {}", input.trim());
                        continue;
                    }
                },
                None => continue,
            };

            // execute command
            match command {
                DebuggerCommand::Quit => break,
                DebuggerCommand::Next => {
                    println!("Executing next.");
                    // Execute the step pointed by current_step.
                    let step = self
                        .debug_config
                        .job
                        .steps
                        .get(current_step)
                        .unwrap()
                        .clone();
                    let exit_code = self.debugger_execute_step(step).await?;
                    if exit_code == ExitCode::SUCCESS {
                        step_success[current_step] = Some(true);
                        if current_step + 1 < self.debug_config.job.steps.len() {
                            current_step += 1;
                        }
                    } else {
                        step_success[current_step] = Some(false);
                    }
                }
                DebuggerCommand::Continue { step_idx } => {
                    println!("Executing continue on {}", step_idx);
                    // Starting with step_idx, execute each step until the end or a failure.
                    current_step = step_idx;
                    loop {
                        if current_step >= self.debug_config.job.steps.len() {
                            break;
                        }

                        let step = self
                            .debug_config
                            .job
                            .steps
                            .get(current_step)
                            .unwrap()
                            .clone();
                        let exit_code = self.debugger_execute_step(step).await?;
                        if exit_code == ExitCode::SUCCESS {
                            step_success[current_step] = Some(true);
                            if current_step + 1 < self.debug_config.job.steps.len() {
                                current_step += 1;
                            } else if current_step + 1 == self.debug_config.job.steps.len() {
                                break;
                            }
                        } else {
                            step_success[current_step] = Some(false);
                            // If a failure occurs, do not advance the current_step.
                            break;
                        }
                    }
                }
                DebuggerCommand::Ssh { step_idx } => {
                    println!("Executing ssh on {}", step_idx);
                    // Open an interactive shell for the step pointed by step_idx.
                    if step_idx < self.debug_config.job.steps.len() {
                        current_step = step_idx;
                        let step = self
                            .debug_config
                            .job
                            .steps
                            .get(current_step)
                            .unwrap()
                            .clone();
                        self.debugger_ssh_step(step).await?;
                    }
                }
                DebuggerCommand::Move { step_idx } => {
                    println!("Executing move on {}", step_idx);
                    // Move current_step to step_idx.
                    if step_idx < self.debug_config.job.steps.len() {
                        current_step = step_idx;
                    } else {
                        println!("Step {} is out of bounds.", step_idx);
                    }
                }
            }

            // update state
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_execute_step(&mut self, step: StepConfig) -> anyhow::Result<ExitCode> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ExecuteStepRequest { step: step },
            ))
            .await?;
        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::ExecuteStepResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })?;
        Ok(ExitCode::SUCCESS)
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_ssh_step(&mut self, step: StepConfig) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ExecuteStepRequest { step: step },
            ))
            .await?;
        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::ExecuteStepResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })?;
        Ok(())
    }
}

pub enum DebuggerCommand {
    Quit,
    Next,
    Continue { step_idx: usize },
    Ssh { step_idx: usize },
    Move { step_idx: usize },
}
