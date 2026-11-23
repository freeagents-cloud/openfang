//! Cancellation coordination across deadline, Server heartbeat/revocation, and OS signals.

use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

/// Reason a running job attempt was cancelled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CancelReason {
    /// Server reported the job was cancelled by the user or control plane.
    ServerCancelled,
    /// Attempt lease expired or job token was revoked (`401`/`403`/`409`/`410`).
    LeaseOrTokenRevoked(u16),
    /// Job exceeded `limits.deadline_ms`.
    DeadlineExceeded,
    /// Process received `SIGTERM` or `SIGINT`.
    Signal,
}

impl CancelReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ServerCancelled => "server_cancelled",
            Self::LeaseOrTokenRevoked(_) => "lease_or_token_revoked",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::Signal => "signal",
        }
    }
}

/// Shared cancellation handle passed to heartbeat, shutdown, deadline, and runner tasks.
#[derive(Debug, Clone)]
pub struct JobCancellation {
    token: CancellationToken,
    reason: Arc<Mutex<Option<CancelReason>>>,
}

impl Default for JobCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl JobCancellation {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            reason: Arc::new(Mutex::new(None)),
        }
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn cancel(&self, reason: CancelReason) {
        let mut guard = self.reason.lock().expect("cancel mutex poisoned");
        if guard.is_none() {
            *guard = Some(reason);
        }
        self.token.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub fn reason(&self) -> Option<CancelReason> {
        self.reason.lock().expect("cancel mutex poisoned").clone()
    }
}
