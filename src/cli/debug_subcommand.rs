use clap::Args;
use http::Request;
use http::StatusCode;
use http_body_util::BodyExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_config::collector::Collector;
use muzanci_config::config::DebugClientConfig;
use muzanci_config::config::JobConfig;
use muzanci_git::GitClient;
use muzanci_git::GitRemote;
use muzanci_observability::TError;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::http::DebugClientResponse;
use muzanci_transport::http::MUZANCI_TRANSPORT_V1;
use muzanci_transport::message::AccountId;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;

use crate::channel::debug_client::DebugClient;
use crate::channel::debug_resolver::DebugResolver;
use crate::cli::parse::parse_env;
use crate::cli::parse::parse_key_val;
use crate::cli::user_config::UserConfig;
use crate::prompt::Intent;
use crate::prompt::prompt_account_selection;
use crate::prompt::prompt_login;
use crate::signal_receiver::SignalReceiver;

#[derive(Args, Debug)]
pub struct DebugSubcommand {
    /// Path to the pipeline config file
    #[arg(long, value_name = "FILE", default_value = "muzan.py")]
    input: PathBuf,

    #[arg(long, value_name = "JOB")]
    job: String,

    #[arg(long, value_parser = parse_key_val, action = clap::ArgAction::Append)]
    env: Vec<(String, String)>,
}

impl DebugSubcommand {
    #[tracing::instrument(skip_all)]
    pub async fn run(self, mut user_config: UserConfig) -> Result<(), TError> {
        let DebugSubcommand { input, job, env } = self;

        let env = parse_env(env)?;

        let collector = Collector::new(&env);

        collector
            .evaluate(&input)
            .map_err(|e| TError::new(&format!("failed to evaluate {}:\n{}", input.display(), e)))?;

        let job = collector
            .jobs()
            .into_iter()
            .find(|j| j.name == job)
            .ok_or_else(|| TError::new(&format!("job not found: {job}")))?;

        println!("Found job: {}", job.name);
        println!("Job steps:");

        for step in &job.steps {
            println!("  - {}", step.name);
        }

        let secret = match user_config.fields().personal_access_token_secret.clone() {
            Some(secret) => secret,
            None => {
                let secret = prompt_login(Intent::Debug)?;
                user_config.fields_mut().personal_access_token_secret = Some(secret.to_string());
                user_config.save()?;
                secret
            }
        };

        let account_id = match user_config.fields().account_id {
            Some(account_id) => account_id,
            None => {
                let account_id = prompt_account_selection(&secret).await?;
                user_config.fields_mut().account_id = Some(account_id);
                user_config.save()?;
                account_id
            }
        };

        let client_shutdown = CancellationToken::new();

        let signal_receiver_enabled = Arc::new(AtomicBool::new(true));
        SignalReceiver::spawn(client_shutdown.clone(), signal_receiver_enabled.clone());

        run_debug_session(
            client_shutdown,
            signal_receiver_enabled,
            secret,
            account_id,
            job,
        )
        .await
    }
}

async fn run_debug_session(
    client_shutdown: CancellationToken,
    signal_receiver_enabled: Arc<AtomicBool>,
    secret: String,
    account_id: AccountId,
    job: JobConfig,
) -> Result<(), TError> {
    let remote = {
        // TODO: Refactor these to be CLI options.
        let target_dir = PathBuf::from("./.git");
        let remote_name = "origin";
        GitClient::try_default()?.get_remote(&target_dir, remote_name)?
    };

    let server_hostname = "localhost:8002";

    let user_id = Uuid::now_v7(); // TODO: Resolve user id from secret.

    let capacity = 1;

    let debug_client_config = run_debug_config_resolver(
        client_shutdown.clone(),
        server_hostname,
        user_id,
        &secret,
        account_id,
        capacity,
    )
    .await?;

    run_debug_client(
        client_shutdown,
        signal_receiver_enabled,
        server_hostname,
        user_id,
        &secret,
        account_id,
        debug_client_config,
        remote,
        job,
    )
    .await?;

    tracing::info!("debug session ended");

    // DebugClient::debugger_control spawns a StdinStream task that blocks on a read syscall and
    //  will not return until stdin is flushed with a newline. To avoid blocking process exit,
    //  we explicitly exit immediately after the debug session ends.
    std::process::exit(0);
}

/// Connects to the server, creates a debug session, and waits for debug config resolution.
///
/// Debug config resolution is necessary because the runner assigned to the debug session
/// may not have the same [`ServerId`] as the server the client is connected to. When a
/// runner is assigned to a debug session, the client will receive a debug config and must
/// connect a debug client to the runner's [`ServerId`] to start the debug session.
async fn run_debug_config_resolver(
    client_shutdown: CancellationToken,
    server_hostname: &str,
    user_id: Uuid,
    secret: &str,
    account_id: AccountId,
    capacity: u64,
) -> Result<DebugClientConfig, TError> {
    let debug_resolver_mux_handle = mux_handle(
        client_shutdown.clone(),
        server_hostname,
        "/begin_debug_resolver",
        &[
            ("x-muzanci-user-id", &user_id.to_string()),
            ("x-muzanci-personal-access-token-secret", secret),
            ("x-muzanci-account-id", &account_id.to_string()),
        ],
    )
    .await?;

    let join_handle = DebugResolver::spawn(
        client_shutdown.clone(),
        debug_resolver_mux_handle,
        user_id,
        account_id,
        capacity,
    );

    match join_handle.await {
        Ok(Ok(debug_client_config)) => Ok(debug_client_config),
        Ok(Err(e)) => Err(e),
        Err(e) => Err(e.into()),
    }
}

async fn run_debug_client(
    client_shutdown: CancellationToken,
    signal_receiver_enabled: Arc<AtomicBool>,
    server_hostname: &str,
    user_id: Uuid,
    secret: &str,
    account_id: AccountId,
    debug_client_config: DebugClientConfig,
    remote: GitRemote,
    job: JobConfig,
) -> Result<(), TError> {
    let debug_resolver_mux_handle = mux_handle(
        client_shutdown.clone(),
        server_hostname,
        "/begin_debug_client",
        &[
            (
                "x-muzanci-server-id",
                &debug_client_config.server_id.to_string(),
            ),
            ("x-muzanci-user-id", &user_id.to_string()),
            ("x-muzanci-personal-access-token-secret", secret),
            ("x-muzanci-account-id", &account_id.to_string()),
        ],
    )
    .await?;

    let join_handle = DebugClient::spawn(
        client_shutdown,
        signal_receiver_enabled,
        debug_resolver_mux_handle,
        user_id,
        account_id,
        debug_client_config.debug_session_id,
        remote,
        job,
    );

    match join_handle.await {
        Ok(result) => result,
        Err(e) => Err(e.into()),
    }
}

#[derive(Debug, thiserror::Error)]
#[error("DebugSubcommandMuxHandleError: {0}")]
struct DebugSubcommandMuxHandleError(String);

async fn mux_handle(
    debug_session_shutdown: CancellationToken,
    server_hostname: &str,
    uri: &str,
    headers: &[(&str, &str)],
) -> Result<MuxHandle, TError> {
    let server_stream = {
        let stream = tokio::net::TcpStream::connect(server_hostname).await?;
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

    let request = {
        let mut request_builder = Request::builder()
            .method("POST")
            .uri(uri)
            .header(http::header::HOST, server_hostname)
            .header(http::header::CONNECTION, "Upgrade")
            .header(http::header::UPGRADE, MUZANCI_TRANSPORT_V1);

        for (name, value) in headers {
            request_builder = request_builder.header(*name, *value);
        }

        request_builder
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .unwrap()
    };

    let response = send_request.send_request(request).await?;

    match response.status() {
        StatusCode::SWITCHING_PROTOCOLS => {}
        StatusCode::BAD_REQUEST => {
            let body_bytes = response.collect().await?.to_bytes();
            let e = serde_json::from_slice::<DebugClientResponse>(&body_bytes)?.error;
            let e = DebugSubcommandMuxHandleError(format!("Client error: {e}"));
            return Err(e.into());
        }
        StatusCode::INTERNAL_SERVER_ERROR => {
            let body_bytes = response.collect().await?.to_bytes();
            let e = serde_json::from_slice::<DebugClientResponse>(&body_bytes)?.error;
            let e = DebugSubcommandMuxHandleError(format!("Server error: {e}"));
            return Err(e.into());
        }
        code => {
            let e = DebugSubcommandMuxHandleError(format!("Unexpected status code: {code}"));
            return Err(e.into());
        }
    }

    let (read_half, write_half) = {
        let hyper_upgraded_stream = hyper::upgrade::on(response).await?;
        let tokio_stream = hyper_util::rt::TokioIo::new(hyper_upgraded_stream);
        tokio::io::split(tokio_stream)
    };

    let channel_acceptor = FnChannelAcceptor::new(move |channel_id, channel_type| {
        panic!(
            "Client received request to open channel [{}] of type {:?}",
            channel_id, channel_type
        );
    });

    let (_join_handle, mux_handle) = Mux::spawn(
        read_half,
        write_half,
        channel_acceptor,
        debug_session_shutdown,
    );

    Ok(mux_handle)
}
