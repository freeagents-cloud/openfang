//! Client helper for fetching a single assigned job from Server (`GET /internal/v1/jobs/{job_id}`).

use crate::protocol::{JobAssignment, PROTOCOL_VERSION};
use anyhow::{bail, Context, Result};
use reqwest::StatusCode;
use std::time::Duration;
use tracing::{info, warn};

const MAX_FETCH_ATTEMPTS: u32 = 3;

/// Fetch the assigned job payload from Server using the job-scoped bearer token.
///
/// Retries only on transient network or 5xx responses; 401/403/404/409 fail immediately.
pub async fn fetch_job(
    client: &reqwest::Client,
    server_url: &str,
    job_id: &str,
    job_token: &str,
) -> Result<JobAssignment> {
    let url = format!(
        "{}/internal/v1/jobs/{}",
        server_url.trim_end_matches('/'),
        job_id
    );

    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=MAX_FETCH_ATTEMPTS {
        let resp = client
            .get(&url)
            .bearer_auth(job_token)
            .header("x-protocol-version", PROTOCOL_VERSION.to_string())
            .send()
            .await;

        match resp {
            Ok(r) if r.status().is_success() => {
                let job: JobAssignment = r
                    .json()
                    .await
                    .context("failed to deserialize JobAssignment from Server")?;
                if job.job_id != job_id {
                    bail!(
                        "Server returned mismatched job_id '{}' (expected '{}')",
                        job.job_id,
                        job_id
                    );
                }
                info!(
                    job_id = %job.job_id,
                    attempt = job.attempt,
                    trace_id = %job.trace_id,
                    "Fetched assigned job from Server"
                );
                return Ok(job);
            }
            Ok(r) => {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                let err = anyhow::anyhow!("GET {url} returned HTTP {status}: {body}");
                if status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS {
                    return Err(err);
                }
                warn!(attempt, %status, "Transient failure fetching job; retrying");
                last_err = Some(err);
            }
            Err(e) => {
                warn!(attempt, error = %e, "Network error fetching job; retrying");
                last_err = Some(e.into());
            }
        }

        if attempt < MAX_FETCH_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("failed to fetch job {job_id}")))
}
