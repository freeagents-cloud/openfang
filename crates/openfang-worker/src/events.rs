//! Ordered, coalesced, idempotent event batch submission and secret redaction.

use crate::cancellation::{CancelReason, JobCancellation};
use crate::protocol::{EventBatchRequest, WorkerEvent, WorkerEventType, PROTOCOL_VERSION};
use chrono::Utc;
use regex_lite::Regex;
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, warn};

const MAX_BATCH_RETRIES: u32 = 4;

/// Handle for emitting ordered job events into the background coalescing batcher.
#[derive(Clone)]
pub struct EventEmitter {
    job_id: String,
    attempt: u32,
    seq: Arc<AtomicU64>,
    tx: mpsc::UnboundedSender<WorkerEvent>,
}

impl EventEmitter {
    /// Emit a single ordered event. Sequence numbers are strictly monotonic per `(job_id, attempt)`.
    pub fn emit(&self, event_type: WorkerEventType, data: Value) {
        let sequence = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        let event = WorkerEvent {
            job_id: self.job_id.clone(),
            attempt: self.attempt,
            sequence,
            event_type,
            at: Utc::now(),
            data,
        };
        let _ = self.tx.send(event);
    }

    /// Emit a text delta event (coalesced by the background flusher).
    pub fn emit_text_delta(&self, delta: &str) {
        if delta.is_empty() {
            return;
        }
        self.emit(WorkerEventType::TextDelta, json!({ "delta": delta }));
    }
}

/// Configuration for the background event batcher.
pub struct EventBatcherConfig {
    pub client: reqwest::Client,
    pub server_url: String,
    pub job_id: String,
    pub attempt: u32,
    pub trace_id: String,
    pub job_token: String,
    pub flush_interval: Duration,
    pub max_batch_size: usize,
    pub cancel: JobCancellation,
}

/// Spawn the background event batcher.
///
/// Drop all clones of the returned [`EventEmitter`] and await the `JoinHandle` to
/// guarantee every queued event has been flushed before sending terminal completion.
pub fn spawn_event_batcher(
    cfg: EventBatcherConfig,
) -> (EventEmitter, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<WorkerEvent>();
    let emitter = EventEmitter {
        job_id: cfg.job_id.clone(),
        attempt: cfg.attempt,
        seq: Arc::new(AtomicU64::new(0)),
        tx,
    };

    let handle = tokio::spawn(async move {
        let mut buffer: Vec<WorkerEvent> = Vec::with_capacity(cfg.max_batch_size);
        let mut interval = tokio::time::interval(cfg.flush_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                maybe_ev = rx.recv() => {
                    match maybe_ev {
                        Some(ev) => {
                            push_coalesced(&mut buffer, ev);
                            if buffer.len() >= cfg.max_batch_size {
                                flush_batch(&cfg, &mut buffer).await;
                            }
                        }
                        None => {
                            // All senders dropped — flush remaining events before exit.
                            if !buffer.is_empty() {
                                flush_batch(&cfg, &mut buffer).await;
                            }
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if !buffer.is_empty() {
                        flush_batch(&cfg, &mut buffer).await;
                    }
                }
            }
        }
    });

    (emitter, handle)
}

/// Coalesce adjacent `TextDelta` events into a single event (keeping the latest sequence number)
/// to avoid sending one HTTP frame per token.
fn push_coalesced(buffer: &mut Vec<WorkerEvent>, incoming: WorkerEvent) {
    if incoming.event_type == WorkerEventType::TextDelta {
        if let Some(last) = buffer.last_mut() {
            if last.event_type == WorkerEventType::TextDelta {
                let prev = last
                    .data
                    .get("delta")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let next = incoming
                    .data
                    .get("delta")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let merged = format!("{prev}{next}");
                last.data = json!({ "delta": merged });
                last.at = incoming.at;
                return;
            }
        }
    }
    buffer.push(incoming);
}

async fn flush_batch(cfg: &EventBatcherConfig, buffer: &mut Vec<WorkerEvent>) {
    if buffer.is_empty() {
        return;
    }
    let events = std::mem::take(buffer);
    let url = format!(
        "{}/internal/v1/jobs/{}/events",
        cfg.server_url.trim_end_matches('/'),
        cfg.job_id
    );
    let payload = EventBatchRequest {
        protocol_version: PROTOCOL_VERSION,
        job_id: cfg.job_id.clone(),
        attempt: cfg.attempt,
        trace_id: cfg.trace_id.clone(),
        events,
    };

    for attempt in 1..=MAX_BATCH_RETRIES {
        match cfg
            .client
            .post(&url)
            .bearer_auth(&cfg.job_token)
            .header("x-protocol-version", PROTOCOL_VERSION.to_string())
            .json(&payload)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                debug!(
                    count = payload.events.len(),
                    job_id = %cfg.job_id,
                    "Flushed event batch"
                );
                return;
            }
            Ok(resp) => {
                let status = resp.status();
                if matches!(
                    status,
                    StatusCode::UNAUTHORIZED
                        | StatusCode::FORBIDDEN
                        | StatusCode::CONFLICT
                        | StatusCode::GONE
                ) {
                    warn!(%status, "Job token revoked or attempt fenced during event flush");
                    cfg.cancel
                        .cancel(CancelReason::LeaseOrTokenRevoked(status.as_u16()));
                    return;
                }
                warn!(attempt, %status, "Event batch submission failed; retrying");
            }
            Err(err) => {
                warn!(attempt, error = %err, "Network error submitting event batch; retrying");
            }
        }

        if attempt < MAX_BATCH_RETRIES {
            tokio::time::sleep(Duration::from_millis(150 * u64::from(attempt))).await;
        }
    }
    warn!(
        job_id = %cfg.job_id,
        count = payload.events.len(),
        "Exhausted retries submitting event batch"
    );
}

/// Redact likely credentials (Bearer tokens, API keys, passwords) from public tool event previews.
pub fn redact_sensitive(input: &str) -> String {
    let mut out = input.to_string();
    for pattern in [
        r"(?i)bearer\s+[a-z0-9._\-]{8,}",
        r"(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*[^\s,;'\x22]{6,}",
        r"sk-[a-zA-Z0-9_\-]{16,}",
    ] {
        if let Ok(re) = Regex::new(pattern) {
            out = re.replace_all(&out, "[REDACTED]").into_owned();
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_bearer_and_api_keys() {
        let s = "Authorization: Bearer super_secret_token_12345 and api_key=abcdef123456";
        let redacted = redact_sensitive(s);
        assert!(!redacted.contains("super_secret_token_12345"));
        assert!(!redacted.contains("abcdef123456"));
        assert!(redacted.contains("[REDACTED]"));
    }
}
