use tracing_error::SpanTrace;

/// A tracing error with logical span hierarchy.
#[derive(Debug)]
pub struct TError {
    type_name: &'static str,
    source: anyhow::Error,
    span_trace: SpanTrace,
}

impl std::fmt::Display for TError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}\nSpan Trace:\n{}",
            self.type_name, self.source, self.span_trace
        )
    }
}

impl<E> From<E> for TError
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn from(err: E) -> Self {
        Self {
            type_name: std::any::type_name::<E>(),
            source: anyhow::Error::new(err),
            span_trace: SpanTrace::capture(),
        }
    }
}
