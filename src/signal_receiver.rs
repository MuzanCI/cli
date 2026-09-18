use std::future::IntoFuture;
use std::sync::Arc;
use tokio::task::JoinHandle;

use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio_util::sync::CancellationToken;

use crate::error::TError;

impl IntoFuture for SignalReceiverHandle {
    type Output = <tokio::task::JoinHandle<()> as IntoFuture>::Output;
    type IntoFuture = tokio::task::JoinHandle<()>;

    fn into_future(self) -> Self::IntoFuture {
        self.handle
    }
}

pub struct SignalReceiverHandle {
    handle: JoinHandle<()>,
}

pub struct SignalReceiver;

#[derive(thiserror::Error, Debug)]
pub enum SignalReceiverError {
    #[error("failed to install SIGINT handler: {0}")]
    SigintInstallFailed(#[source] std::io::Error),

    #[error("failed to install SIGTERM handler: {0}")]
    SigtermInstallFailed(#[source] std::io::Error),
}

impl SignalReceiver {
    pub fn spawn(cancellation_token: CancellationToken) -> SignalReceiverHandle {
        let handle = tokio::spawn(async move {
            SignalReceiver.run(cancellation_token).await.unwrap();
        });
        SignalReceiverHandle { handle }
    }

    #[tracing::instrument(skip_all)]
    async fn run(self, cancellation_token: CancellationToken) -> Result<(), TError> {
        tracing::info!("starting main...");
        let cancellation_token_clone = cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token_clone.cancelled() => {
                tracing::info!("SignalReceiver received cancellation signal.");
                Ok(())
            }
            result = self.main(cancellation_token) => {
                match result {
                    Ok(_) => {
                        tracing::info!("SignalReceiver finished running");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("SignalReceiver encountered an error: {:?}", e);
                        Err(e)
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn main(self, cancellation_token: CancellationToken) -> Result<(), TError> {
        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGINT handler: {e}");
                cancellation_token.cancel();
                Err(SignalReceiverError::SigintInstallFailed(e))?
            }
        };

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGTERM handler: {e}");
                cancellation_token.cancel();
                Err(SignalReceiverError::SigtermInstallFailed(e))?
            }
        };

        tokio::select! {
            _ = sigint.recv() => {
                tracing::warn!("SIGINT received, shutting down...");
                cancellation_token.cancel();
            }
            _ = sigterm.recv() => {
                tracing::warn!("SIGTERM received, shutting down...");
                cancellation_token.cancel();
            }
        }

        Ok(())
    }
}
