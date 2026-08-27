use std::path::PathBuf;

use muzanci_config::JobConfig;
use muzanci_config::StepConfig;
use muzanci_config::config::DebugSessionId;
use muzanci_git::GitClient;
use muzanci_git::GitRemote;
use muzanci_transport::message::DebugClientMessage;
use muzanci_transport::message::Message;
use sha2::Digest;
use sha2::Sha256;
use tempfile::NamedTempFile;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio_util::sync::CancellationToken;

use muzanci_config::config::ImageConfig;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::mux::MuxHandle;

use crate::debug::debug_client_tunnel::tunnel_exec;
use crate::debug::debug_client_tunnel::tunnel_interactive;
use crate::stdin::StdinStream;

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
    debug_session_id: DebugSessionId,
    remote: GitRemote,
    job: JobConfig,
    diff_file: Option<NamedTempFile>,
    diff_hasher: Option<Sha256>,
}

impl DebugClient {
    pub fn spawn(
        mux_handle: MuxHandle,
        cancellation_token: CancellationToken,
        debug_session_id: DebugSessionId,
        remote: GitRemote,
        job: JobConfig,
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
                debug_session_id,
                remote,
                job,
                diff_file: None,
                diff_hasher: None,
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
        self.connect_debug_client().await?;
        self.create_sandbox(self.job.image.clone()).await?;
        self.checkout_branch().await?;
        self.create_diff().await?;
        self.start_diff_upload().await?;
        self.send_diff().await?;
        self.complete_diff_upload().await?;
        self.apply_diff().await?;
        self.debugger_control().await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn connect_debug_client(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ConnectDebugClientRequest {
                    debug_session_id: self.debug_session_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::ConnectDebugClientResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn create_sandbox(&mut self, image: ImageConfig) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CreateSandboxRequest { image },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::CreateSandboxResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn checkout_branch(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CheckoutBranchRequest {
                    url: self.remote.url.clone(),
                    branch: self.remote.branch.clone(),
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
        let mut diff_file = tempfile::NamedTempFile::new()?;
        let git_client = GitClient::try_default()?;
        let target_dir = PathBuf::from("./.git");
        git_client.create_diff(&target_dir, self.remote.branch.clone(), &mut diff_file)?;
        self.diff_file = Some(diff_file);
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn start_diff_upload(&mut self) -> anyhow::Result<()> {
        self.diff_hasher = Some(Sha256::new());

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
        let diff_file = {
            let file = self
                .diff_file
                .as_ref()
                .ok_or(anyhow::anyhow!("No diff file"))?;
            tokio::fs::File::from_std(file.reopen()?)
        };

        const CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
        let mut reader = BufReader::with_capacity(CHUNK_SIZE, diff_file);
        loop {
            let buffer = reader.fill_buf().await?;
            let n = buffer.len();
            if n == 0 {
                break;
            }

            {
                let hasher = self
                    .diff_hasher
                    .as_mut()
                    .ok_or_else(|| anyhow::anyhow!("No diff hasher"))?;
                hasher.update(buffer);
            }

            let chunk = buffer.to_vec();
            self.channel_tx
                .send(Message::DebugClient(
                    DebugClientMessage::UploadDiffChunkRequest { chunk },
                ))
                .await?;

            self.channel_rx
                .recv()
                .await
                .ok_or(anyhow::anyhow!("Channel closed"))
                .and_then(|response| match response {
                    Message::DebugClient(DebugClientMessage::UploadDiffChunkResponse {
                        result,
                    }) => result.map_err(|e| anyhow::anyhow!(e)),
                    _ => Err(anyhow::anyhow!("Unexpected message type")),
                })?;

            reader.consume(n);
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn complete_diff_upload(&mut self) -> anyhow::Result<()> {
        let checksum = {
            let diff_hasher = self
                .diff_hasher
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("No diff hasher"))?;
            hex::encode(diff_hasher.finalize_reset())
        };

        tracing::info!("Diff checksum [{}]", checksum);

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CompleteDiffUploadRequest { checksum },
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
    async fn apply_diff(&mut self) -> anyhow::Result<()> {
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
            })
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_control(&mut self) -> anyhow::Result<()> {
        let mut stdin = StdinStream::new(self.cancellation_token.clone());

        let mut current_step: usize = 0;
        let mut step_success: Vec<Option<bool>> = vec![None; self.job.steps.len()];

        loop {
            // print current state
            println!("Job: {}", self.job.name);
            println!("Steps:");
            for (i, step) in self.job.steps.iter().enumerate() {
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
            tracing::info!("input.clear");
            let line = stdin.read_line().await?;
            tracing::info!("stdin.read_line: [{}]", line);

            // parse into command
            let mut parts = line.trim().split_whitespace();
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
                        println!("Unknown command: {}", line.trim());
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
                    let step = self.job.steps.get(current_step).unwrap().clone();
                    let exit_code = self.debugger_execute_step(step).await?;
                    if exit_code == 0 {
                        step_success[current_step] = Some(true);
                        if current_step + 1 < self.job.steps.len() {
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
                        if current_step >= self.job.steps.len() {
                            break;
                        }

                        let step = self.job.steps.get(current_step).unwrap().clone();
                        let exit_code = self.debugger_execute_step(step).await?;
                        if exit_code == 0 {
                            step_success[current_step] = Some(true);
                            if current_step + 1 < self.job.steps.len() {
                                current_step += 1;
                            } else if current_step + 1 == self.job.steps.len() {
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
                    if step_idx < self.job.steps.len() {
                        current_step = step_idx;
                        let step = self.job.steps.get(current_step).unwrap().clone();
                        self.debugger_ssh_step(&mut stdin, step).await?;
                    }
                }
                DebuggerCommand::Move { step_idx } => {
                    println!("Executing move on {}", step_idx);
                    // Move current_step to step_idx.
                    if step_idx < self.job.steps.len() {
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
    async fn debugger_execute_step(&mut self, step: StepConfig) -> anyhow::Result<u32> {
        let cmd = step.command.clone();

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartShellRequest { step },
            ))
            .await?;
        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::StartShellResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })?;

        tunnel_exec(&cmd, self.mux_handle.clone(), self.debug_session_id.clone()).await
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_ssh_step(
        &mut self,
        stdin: &mut StdinStream,
        step: StepConfig,
    ) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartShellRequest { step },
            ))
            .await?;
        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::StartShellResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })?;

        tunnel_interactive(
            stdin,
            self.mux_handle.clone(),
            self.debug_session_id.clone(),
        )
        .await
    }
}

pub enum DebuggerCommand {
    Quit,
    Next,
    Continue { step_idx: usize },
    Ssh { step_idx: usize },
    Move { step_idx: usize },
}
