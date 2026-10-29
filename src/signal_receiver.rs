use std::sync::Arc;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio::task::AbortHandle;

use muzanci_observability::TError;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(thiserror::Error, Debug)]
pub enum SignalReceiverError {
    #[error("failed to install SIGINT handler: {0}")]
    SigintInstallFailed(#[source] std::io::Error),

    #[error("failed to install SIGTERM handler: {0}")]
    SigtermInstallFailed(#[source] std::io::Error),
}

pub struct SignalReceiver {
    debug_session_shutdown: CancellationToken,
}

impl SignalReceiver {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        debug_session_shutdown: CancellationToken,
    ) -> AbortHandle {
        join_set.spawn(async move {
            SignalReceiver {
                debug_session_shutdown,
            }
            .main()
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "signal_receiver"
        fields(
            id = Uuid::now_v7().to_string(),
            debug_session_id = "TODO",
        )
    )]
    async fn main(mut self) -> Result<(), TError> {
        tracing::info!("signal_receiver.started");

        let debug_session_shutdown = self.debug_session_shutdown.clone();

        tokio::select! {
            _ = debug_session_shutdown.cancelled() => {
                tracing::info!("SignalReceiver received runner shutdown signal.");
            }
            result = self.run() => {
                result?
            }
        }

        tracing::info!("signal_receiver.finished");

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self) -> Result<(), TError> {
        let debug_session_shutdown = self.debug_session_shutdown.clone();

        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                debug_session_shutdown.cancel();
                let e = SignalReceiverError::SigintInstallFailed(e);
                return Err(e.into());
            }
        };

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                debug_session_shutdown.cancel();
                let e = SignalReceiverError::SigtermInstallFailed(e);
                return Err(e.into());
            }
        };

        tokio::select! {
            _ = sigint.recv() => {
                tracing::warn!("SIGINT received, shutting down runner...");
                debug_session_shutdown.cancel();
            }
            _ = sigterm.recv() => {
                tracing::warn!("SIGTERM received, shutting down runner...");
                debug_session_shutdown.cancel();
            }
        }

        Ok(())
    }
}
