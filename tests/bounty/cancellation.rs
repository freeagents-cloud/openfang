mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_worker::protocol::TerminalStatus;
use openfang_worker::runner::run_single_job;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn deadline_exceeded_cancels_loop_and_reports_cancelled() {
    let state = MockServerState::new();
    // Simulate a slow inference response that exceeds deadline_ms.
    state.inference_delay_ms.store(3_000, Ordering::SeqCst);
    state.push_text_sse_response(&["Too late"]);

    let mut job = sample_job("job-deadline-1", "Take too long");
    job.limits.deadline_ms = 250;
    state.insert_job(job);

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-deadline");

    let outcome = run_single_job(&cfg, "job-deadline-1")
        .await
        .expect("returns cancelled outcome");

    assert_eq!(outcome.status, TerminalStatus::Cancelled);
    assert_eq!(outcome.finish_reason, "deadline_exceeded");

    let completions = state.completions.lock().unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, TerminalStatus::Cancelled);
}

#[tokio::test]
async fn heartbeat_cancellation_stops_active_job() {
    let state = MockServerState::new();
    state.inference_delay_ms.store(3_000, Ordering::SeqCst);
    state.cancel_on_heartbeat.store(true, Ordering::SeqCst);
    state.insert_job(sample_job("job-hb-cancel-1", "Cancel me via heartbeat"));

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let mut cfg = sample_worker_config(server_url, "tok-hb-cancel");
    cfg.heartbeat_interval_ms = 100;

    let outcome = run_single_job(&cfg, "job-hb-cancel-1")
        .await
        .expect("returns cancelled outcome");

    assert_eq!(outcome.status, TerminalStatus::Cancelled);
    assert_eq!(outcome.finish_reason, "server_cancelled");
}

#[tokio::test]
async fn revoked_token_on_heartbeat_cancels_active_job() {
    let state = MockServerState::new();
    state.inference_delay_ms.store(3_000, Ordering::SeqCst);
    *state.heartbeat_error_status.lock().unwrap() = Some(401);
    state.insert_job(sample_job("job-hb-401", "Revoke my token"));

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let mut cfg = sample_worker_config(server_url, "tok-revoked");
    cfg.heartbeat_interval_ms = 100;

    let outcome = run_single_job(&cfg, "job-hb-401")
        .await
        .expect("returns cancelled outcome");

    assert_eq!(outcome.status, TerminalStatus::Cancelled);
    assert_eq!(outcome.finish_reason, "lease_or_token_revoked");
}
