//! Periodic attempt-lease heartbeat task (`POST /internal/v1/jobs/{job_id}/heartbeat`).

use crate::cancellation::{CancelReason, JobCancellation};
use crate::protocol::{HeartbeatRequest, HeartbeatResponse, PROTOCOL_VERSION};
use chrono::Utc;
use reqwest::StatusCode;
use std::time::Duration;
use tracing::{debug, warn};

/// Configuration for the heartbeat loop.
pub struct HeartbeatConfig {
    pub client: reqwest::Client,
    pub server_url: String,
    pub job_id: String,
    pub attempt: u32,
    pub trace_id: String,
    pub job_token: String,
    pub interval: Duration,
    pub cancel: JobCancellation,
}

/// Spawn a periodic heartbeat task that extends the attempt lease and detects cancellation
/// or token/lease revocation.
pub fn spawn_heartbeat(cfg: HeartbeatConfig) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let url = format!(
            "{}/internal/v1/jobs/{}/heartbeat",
            cfg.server_url.trim_end_matches('/'),
            cfg.job_id
        );
        let mut ticker = tokio::time::interval(cfg.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Skip immediate tick at t=0; first heartbeat fires after one interval.
        ticker.tick().await;

        let cancel_token = cfg.cancel.token();
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    debug!(job_id = %cfg.job_id, "Heartbeat task stopping (cancelled)");
                    break;
                }
                _ = ticker.tick() => {
                    let body = HeartbeatRequest {
                        protocol_version: PROTOCOL_VERSION,
                        job_id: cfg.job_id.clone(),
                        attempt: cfg.attempt,
                        trace_id: cfg.trace_id.clone(),
                        at: Utc::now(),
                    };
                    match cfg
                        .client
                        .post(&url)
                        .bearer_auth(&cfg.job_token)
                        .header("x-protocol-version", PROTOCOL_VERSION.to_string())
                        .json(&body)
                        .send()
                        .await
                    {
                        Ok(resp) if resp.status().is_success() => {
                            if let Ok(parsed) = resp.json::<HeartbeatResponse>().await {
                                if parsed.cancelled {
                                    warn!(job_id = %cfg.job_id, "Server signaled job cancellation via heartbeat");
                                    cfg.cancel.cancel(CancelReason::ServerCancelled);
                                    break;
                                }
                            }
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
                                warn!(job_id = %cfg.job_id, %status, "Heartbeat rejected; revoking local execution");
                                cfg.cancel.cancel(CancelReason::LeaseOrTokenRevoked(status.as_u16()));
                                break;
                            }
                            warn!(job_id = %cfg.job_id, %status, "Transient heartbeat error");
                        }
                        Err(err) => {
                            warn!(job_id = %cfg.job_id, error = %err, "Network error during heartbeat");
                        }
                    }
                }
            }
        }
    })
}
