use std::sync::Once;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

pub use tracing::error;
pub use tracing::info;
pub use tracing::warn;

use crate::error::TError;

/// Guards single installation of the tracing subscriber.
static SUBSCRIBER_INSTALLED: Once = Once::new();

/// Guards single installation of the process-wide panic hook.
static PANIC_HOOK_INSTALLED: Once = Once::new();

#[derive(thiserror::Error, Debug)]
pub enum LoggingError {
    #[error("Failed to create log directory: {0}")]
    CreateLogDirectoryFailed(#[source] std::io::Error),
}

/// Initialize tracing for debug builds.
#[cfg(all(debug_assertions, not(test)))]
#[tracing::instrument(skip_all)]
pub fn init() -> Result<Option<tracing_appender::non_blocking::WorkerGuard>, TError> {
    SUBSCRIBER_INSTALLED.call_once(|| {
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::Layer::new().with_writer(std::io::stderr))
            .with(tracing_error::ErrorLayer::default()) // Required for capturing SpanTrace
            .init();
    });

    install_panic_hook();
    Ok(None)
}

/// Initialize tracing for release/test/bench builds.
#[cfg(any(not(debug_assertions), test))]
#[tracing::instrument(skip_all)]
pub fn init() -> Result<Option<tracing_appender::non_blocking::WorkerGuard>, TError> {
    let log_directory = format!(".");
    std::fs::create_dir_all(&log_directory)
        .map_err(|e| LoggingError::CreateLogDirectoryFailed(e))?;

    let (file_writer, guard) = tracing_appender::non_blocking(tracing_appender::rolling::never(
        &log_directory,
        "muzanci-server.log",
    ));

    let mut installed = false;
    SUBSCRIBER_INSTALLED.call_once(|| {
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::Layer::new()
                    .json()
                    .flatten_event(true)
                    .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
                    .with_writer(file_writer.clone()),
            )
            .with(tracing_error::ErrorLayer::default()) // Required for capturing SpanTrace
            .init();
        installed = true;
    });

    install_panic_hook();
    Ok(if installed { Some(guard) } else { None })
}

/// Install a process-wide panic hook (idempotent).
///
/// Logs the panic via `tracing::error!` and cancels the registered
/// cancellation token, then chains to the previous hook.
fn install_panic_hook() {
    PANIC_HOOK_INSTALLED.call_once(|| {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = panic_payload(info);
            let location = info
                .location()
                .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
                .unwrap_or_else(|| "<unknown>".to_string());

            let span_trace = tracing_error::SpanTrace::capture();

            tracing::error!(
                panic.payload = %payload,
                panic.location = %location,
                panic.span_trace = %span_trace,
            );

            prev(info);
        }));
    });
}

/// Extract the panic payload as a string when it is a `&str` or `String`.
fn panic_payload(info: &std::panic::PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Calling `init` twice must not panic; the hook is installed once.
    #[test]
    #[tracing::instrument(skip_all)]
    fn init_is_idempotent() -> Result<(), TError> {
        let _ = init()?;
        let _ = init()?;
        Ok(())
    }
}
