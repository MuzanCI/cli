use std::io;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Centralized Stdin handle passed around the application
pub struct StdinStream {
    rx: mpsc::Receiver<Vec<u8>>,
    buffer: Vec<u8>, // Remainder buffer for CLI line parsing
}

impl StdinStream {
    pub fn new(cancellation_token: CancellationToken) -> Self {
        let (tx, rx) = mpsc::channel(32);

        // Long-lived background task running for the entire application duration
        tokio::spawn(async move {
            let mut stdin = tokio::io::stdin(); // NOTE: No BufReader!
            let mut buf = [0u8; 1024];
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => break,
                    result = stdin.read(&mut buf) => {
                        match result {
                            Ok(n) => {
                                if tx.send(buf[..n].to_vec()).await.is_err() {
                                    break; // App exiting
                                }
                            }
                            _ => break,
                        }
                    }
                }
            }
        });

        Self {
            rx,
            buffer: Vec::new(),
        }
    }

    /// Read raw bytes for the interactive SSH shell
    pub async fn recv_raw(&mut self) -> Option<Vec<u8>> {
        // Drain any leftover buffered bytes first
        if !self.buffer.is_empty() {
            return Some(std::mem::take(&mut self.buffer));
        }
        self.rx.recv().await
    }

    /// Read a line-buffered String for the parent CLI task
    pub async fn read_line(&mut self) -> io::Result<String> {
        loop {
            // Check if we already have a full line in our remainder buffer
            if let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
                let line_bytes = self.buffer.drain(..=pos).collect::<Vec<u8>>();
                let line_str = String::from_utf8(line_bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                return Ok(line_str);
            }

            // Wait for next byte chunk from the single stdin task
            match self.rx.recv().await {
                Some(chunk) => self.buffer.extend_from_slice(&chunk),
                None => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "EOF")),
            }
        }
    }
}
