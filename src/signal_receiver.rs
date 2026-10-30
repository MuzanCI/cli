use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use uuid::Uuid;

use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio::task::AbortHandle;

use tokio_util::sync::CancellationToken;

use muzanci_observability::TError;

#[derive(thiserror::Error, Debug)]
pub enum SignalReceiverError {
    #[error("failed to install SIGINT handler: {0}")]
    SigintInstallFailed(#[source] std::io::Error),

    #[error("failed to install SIGTERM handler: {0}")]
    SigtermInstallFailed(#[source] std::io::Error),
}

pub struct SignalReceiver {
    debug_session_shutdown: CancellationToken,
    enabled: Arc<AtomicBool>,
}

impl SignalReceiver {
    pub fn spawn(
        debug_session_shutdown: CancellationToken,
        enabled: Arc<AtomicBool>,
    ) -> AbortHandle {
        tokio::spawn(async move {
            SignalReceiver {
                debug_session_shutdown,
                enabled,
            }
            .main()
            .await
        })
        .abort_handle()
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

        loop {
            tokio::select! {
                _ = sigint.recv() => {
                    tracing::warn!("SIGINT received, shutting down runner...");
                }
                _ = sigterm.recv() => {
                    tracing::warn!("SIGTERM received, shutting down runner...");
                }
            }

            if self.enabled.load(std::sync::atomic::Ordering::Relaxed) {
                debug_session_shutdown.cancel();
                break;
            }
        }

        Ok(())
    }
}
