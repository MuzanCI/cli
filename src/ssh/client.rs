use crossterm::terminal;
use russh::client::Handle;
use russh::keys::PrivateKeyWithHashAlg;
use std::io::Write;
use std::io::stdout;
use std::sync::Arc;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;

use crate::stdin::StdinStream;

pub struct ClientHandler;

impl russh::client::Handler for ClientHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

pub async fn establish_ssh_session<S>(
    stream: S,
    private_key: PrivateKeyWithHashAlg,
    username: &str,
) -> Result<Handle<ClientHandler>, Box<dyn std::error::Error>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let config = Arc::new(russh::client::Config::default());
    let handler = ClientHandler;

    // Connect over the custom MPSC transport stream
    let mut session = russh::client::connect_stream(config, stream, handler).await?;

    // Authenticate using public key
    let auth_res = session
        .authenticate_publickey(username, private_key)
        .await?;

    if !auth_res.success() {
        return Err("SSH Authentication failed".into());
    }

    Ok(session)
}

pub async fn run_interactive_shell(
    stdin: &mut StdinStream,
    session: &mut Handle<ClientHandler>,
) -> Result<u32, Box<dyn std::error::Error>> {
    // 1. Open an SSH session channel
    let mut channel = session.channel_open_session().await?;

    // 2. Request PTY and launch the interactive shell
    let (cols, rows) = terminal::size().unwrap_or((80, 24));
    channel
        .request_pty(false, "xterm-256color", cols as u32, rows as u32, 0, 0, &[])
        .await?;
    channel.request_shell(true).await?;

    // 3. Enable Terminal Raw Mode locally
    terminal::enable_raw_mode()?;

    let mut exit_code: u32 = 0;

    // 4. Main IO Loop
    loop {
        tokio::select! {
            // Path A: Read local stdin -> Send to SSH channel
            data_opt = stdin.recv_raw() => {
                match data_opt {
                    Some(data) => {
                        channel.data(&data[..]).await?;
                    }
                    None => {
                        break; // Receiver dropped (session ended)
                    }
                }
            }

            // Path B: Read remote SSH channel events -> Print to local stdout / Detect Exit
            msg_opt = channel.wait() => {
                match msg_opt {
                    Some(russh::ChannelMsg::Data { ref data }) => {
                        let mut out = stdout();
                        out.write_all(data)?;
                        out.flush()?;
                    }
                    Some(russh::ChannelMsg::ExtendedData { ref data, .. }) => {
                        let mut out = stdout();
                        out.write_all(data)?;
                        out.flush()?;
                    }
                    Some(russh::ChannelMsg::ExitStatus { exit_status }) => {
                        // Capture exit code sent by the server process
                        exit_code = exit_status;
                    }
                    Some(russh::ChannelMsg::Eof) | Some(russh::ChannelMsg::Close) | None => {
                        // Shell terminal session ended on server host
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    tracing::info!("aborted stdin_task");

    // 5. Restore Terminal Normal Mode
    let _ = terminal::disable_raw_mode();
    tracing::info!("Disabled raw mode");

    Ok(exit_code)
}
