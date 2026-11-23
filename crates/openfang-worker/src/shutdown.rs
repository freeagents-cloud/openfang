//! Graceful OS signal handling (`SIGINT` / `SIGTERM`) for `openfang-worker`.

use crate::cancellation::{CancelReason, JobCancellation};
use tracing::warn;

/// Spawn a background task that cancels `cancel` when `SIGINT` or `SIGTERM` arrives.
pub fn spawn_signal_watcher(cancel: JobCancellation) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let cancel_token = cancel.token();
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).ok();
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    warn!("Received SIGINT; cancelling active job");
                    cancel.cancel(CancelReason::Signal);
                }
                _ = async {
                    if let Some(ref mut s) = sigterm {
                        s.recv().await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    warn!("Received SIGTERM; cancelling active job");
                    cancel.cancel(CancelReason::Signal);
                }
                _ = cancel_token.cancelled() => {}
            }
        }
        #[cfg(not(unix))]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {
                    warn!("Received Ctrl+C; cancelling active job");
                    cancel.cancel(CancelReason::Signal);
                }
                _ = cancel_token.cancelled() => {}
            }
        }
    })
}
