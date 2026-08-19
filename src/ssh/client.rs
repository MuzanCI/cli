use crossterm::terminal;
use russh::client::Handle;
use std::io::Write;
use std::process::ExitCode;

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

pub async fn run_exec_shell(
    cmd: &str,
    session: &mut Handle<ClientHandler>,
) -> Result<u32, Box<dyn std::error::Error>> {
    let mut channel = session.channel_open_session().await?;
    channel.exec(true, cmd).await?;

    let mut exit_code: u32 = 0;

    loop {
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if result.is_ok() {
                    tracing::info!("Forwarding Ctrl+C to remote process");
                    let _ = channel.signal(russh::Sig::INT).await;
                }
            }
            msg_opt = channel.wait() => {
                match msg_opt {
                    Some(russh::ChannelMsg::Data { ref data }) => {
                        let mut out = std::io::stdout();
                        out.write_all(data)?;
                        out.flush()?;
                    }
                    Some(russh::ChannelMsg::ExtendedData { ref data, .. }) => {
                        let mut out = std::io::stderr();
                        out.write_all(data)?;
                        out.flush()?;
                    }
                    Some(russh::ChannelMsg::ExitStatus { exit_status }) => {
                        exit_code = exit_status;
                    }
                    Some(russh::ChannelMsg::Eof) | Some(russh::ChannelMsg::Close) | None => {
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(exit_code)
}

pub async fn run_interactive_shell(
    stdin: &mut StdinStream,
    session: &mut Handle<ClientHandler>,
) -> Result<u32, Box<dyn std::error::Error>> {
    let mut channel = session.channel_open_session().await?;

    let (cols, rows) = terminal::size().unwrap_or((80, 24));
    channel
        .request_pty(false, "xterm-256color", cols as u32, rows as u32, 0, 0, &[])
        .await?;
    channel.request_shell(true).await?;

    terminal::enable_raw_mode()?;

    let mut exit_code: u32 = 0;

    loop {
        tokio::select! {
            data_opt = stdin.recv_raw() => {
                match data_opt {
                    Some(data) => {
                        channel.data(&data[..]).await?;
                    }
                    None => {
                        break;
                    }
                }
            }

            msg_opt = channel.wait() => {
                match msg_opt {
                    Some(russh::ChannelMsg::Data { ref data }) => {
                        let mut out = std::io::stdout();
                        out.write_all(data)?;
                        out.flush()?;
                    }
                    Some(russh::ChannelMsg::ExtendedData { ref data, .. }) => {
                        let mut out = std::io::stderr();
                        out.write_all(data)?;
                        out.flush()?;
                    }
                    Some(russh::ChannelMsg::ExitStatus { exit_status }) => {
                        exit_code = exit_status;
                    }
                    Some(russh::ChannelMsg::Eof) | Some(russh::ChannelMsg::Close) | None => {
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    let _ = terminal::disable_raw_mode();

    Ok(exit_code)
}
