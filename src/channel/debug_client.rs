use sha2::Digest;
use sha2::Sha256;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tempfile::NamedTempFile;
use tokio::io::AsyncBufReadExt;
use tokio::io::BufReader;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_config::config::DebugSessionId;
use muzanci_config::config::ImageConfig;
use muzanci_config::config::JobConfig;
use muzanci_config::config::StepConfig;
use muzanci_git::GitClient;
use muzanci_git::GitRemote;
use muzanci_observability::TError;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::AccountId;
use muzanci_transport::message::DebugClientMessage;
use muzanci_transport::message::Message;
use muzanci_transport::mux::MuxHandle;

use crate::channel::debug_client_tunnel::tunnel_exec;
use crate::channel::debug_client_tunnel::tunnel_interactive;
use crate::stdin::StdinStream;

pub struct DebugClient {
    client_shutdown: CancellationToken,
    signal_receiver_enabled: Arc<AtomicBool>,
    mux_handle: MuxHandle,
    channel_tx: ChannelSender,
    user_id: Uuid,
    account_id: AccountId,
    debug_session_id: DebugSessionId,
    remote: GitRemote,
    job: JobConfig,
    diff_file: Option<NamedTempFile>,
    diff_hasher: Option<Sha256>,
}

impl DebugClient {
    pub fn spawn(
        client_shutdown: CancellationToken,
        signal_receiver_enabled: Arc<AtomicBool>,
        mux_handle: MuxHandle,
        user_id: Uuid,
        account_id: AccountId,
        debug_session_id: DebugSessionId,
        remote: GitRemote,
        job: JobConfig,
    ) -> JoinHandle<Result<(), TError>> {
        tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) =
                mux_handle.open_channel(ChannelType::DebugClient).await?;

            DebugClient {
                client_shutdown,
                signal_receiver_enabled,
                mux_handle,
                channel_tx,
                user_id,
                account_id,
                debug_session_id,
                remote,
                job,
                diff_file: None,
                diff_hasher: None,
            }
            .main(channel_rx, channel_closed)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "debug_client",
        fields(
            id = Uuid::now_v7().to_string(),
            user_id = self.user_id.to_string(),
            account_id = self.account_id.to_string(),
            debug_session_id = self.debug_session_id.to_string(),
        )
    )]
    async fn main(
        mut self,
        channel_rx: ChannelReceiver,
        channel_closed: CancellationToken,
    ) -> Result<(), TError> {
        let client_shutdown = self.client_shutdown.clone();
        tokio::select! {
            _ = client_shutdown.cancelled() => {
                tracing::info!("received client shutdown signal.");
            }

            _ = channel_closed.cancelled() => {
                tracing::info!("DebugClient channel closed. Stopping DebugClient task.");
            }

            result = self.run(channel_rx) => {
                result?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self, mut channel_rx: ChannelReceiver) -> Result<(), TError> {
        self.connect_debug_client(&mut channel_rx).await?;
        self.create_sandbox(&mut channel_rx, self.job.image.clone())
            .await?;
        self.checkout_branch(&mut channel_rx).await?;
        self.create_diff().await?;
        self.start_diff_upload(&mut channel_rx).await?;
        self.send_diff(&mut channel_rx).await?;
        self.complete_diff_upload(&mut channel_rx).await?;
        self.apply_diff(&mut channel_rx).await?;
        self.debugger_control(&mut channel_rx).await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn connect_debug_client(
        &mut self,
        channel_rx: &mut ChannelReceiver,
    ) -> Result<(), TError> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ConnectDebugClientRequest {
                    debug_session_id: self.debug_session_id,
                },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::ConnectDebugClientResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn create_sandbox(
        &mut self,
        channel_rx: &mut ChannelReceiver,
        image: ImageConfig,
    ) -> Result<(), TError> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CreateSandboxRequest { image },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::CreateSandboxResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn checkout_branch(&mut self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CheckoutBranchRequest {
                    url: self.remote.url.clone(),
                    branch: self.remote.branch.clone(),
                },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::CheckoutBranchResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn create_diff(&mut self) -> Result<(), TError> {
        let mut diff_file = tempfile::NamedTempFile::new()?;
        let git_client = GitClient::try_default()?;
        let target_dir = PathBuf::from("./.git");
        git_client.create_diff(&target_dir, self.remote.branch.clone(), &mut diff_file)?;
        self.diff_file = Some(diff_file);
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn start_diff_upload(&mut self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        self.diff_hasher = Some(Sha256::new());

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartDiffUploadRequest,
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::StartDiffUploadResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn send_diff(&mut self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        let diff_file = {
            let file = self
                .diff_file
                .as_ref()
                .ok_or_else(|| TError::new("No diff file"))?;
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
                    .ok_or_else(|| TError::new("No diff hasher"))?;
                hasher.update(buffer);
            }

            let chunk = buffer.to_vec();
            self.channel_tx
                .send(Message::DebugClient(
                    DebugClientMessage::UploadDiffChunkRequest { chunk },
                ))
                .await?;

            channel_rx
                .recv()
                .await
                .ok_or_else(|| TError::new("Channel closed"))
                .and_then(|response| match response {
                    Message::DebugClient(DebugClientMessage::UploadDiffChunkResponse {
                        result,
                    }) => result.map_err(|e| TError::new(&e.to_string())),
                    _ => Err(TError::new("Unexpected message type")),
                })?;

            reader.consume(n);
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn complete_diff_upload(
        &mut self,
        channel_rx: &mut ChannelReceiver,
    ) -> Result<(), TError> {
        let checksum = {
            let diff_hasher = self
                .diff_hasher
                .as_mut()
                .ok_or_else(|| TError::new("No diff hasher"))?;
            hex::encode(diff_hasher.finalize_reset())
        };

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CompleteDiffUploadRequest { checksum },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::CompleteDiffUploadResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn apply_diff(&mut self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        self.channel_tx
            .send(Message::DebugClient(DebugClientMessage::ApplyDiffRequest))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::ApplyDiffResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_control(&mut self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        let mut stdin = StdinStream::new(self.client_shutdown.clone());

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
            let line = stdin.read_line().await?;

            // parse into command
            let mut parts = line.split_whitespace();
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
                    let exit_code = self.debugger_execute_step(channel_rx, step).await?;
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
                        let exit_code = self.debugger_execute_step(channel_rx, step).await?;
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
                        self.debugger_ssh_step(channel_rx, &mut stdin, step).await?;
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
    async fn debugger_execute_step(
        &mut self,
        channel_rx: &mut ChannelReceiver,
        step: StepConfig,
    ) -> Result<u32, TError> {
        let cmd = step.command.clone();

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartShellRequest { step },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::StartShellResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })?;

        tunnel_exec(
            self.signal_receiver_enabled.clone(),
            &cmd,
            self.mux_handle.clone(),
            self.user_id,
            self.account_id,
            self.debug_session_id,
        )
        .await
    }

    #[tracing::instrument(skip_all)]
    async fn debugger_ssh_step(
        &mut self,
        channel_rx: &mut ChannelReceiver,
        stdin: &mut StdinStream,
        step: StepConfig,
    ) -> Result<(), TError> {
        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartShellRequest { step },
            ))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebugClient(DebugClientMessage::StartShellResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })?;

        tunnel_interactive(
            self.signal_receiver_enabled.clone(),
            stdin,
            self.mux_handle.clone(),
            self.user_id,
            self.account_id,
            self.debug_session_id,
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
