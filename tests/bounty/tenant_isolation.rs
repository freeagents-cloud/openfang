mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_worker::protocol::TerminalStatus;
use openfang_worker::runner::run_single_job;

#[tokio::test]
async fn concurrent_jobs_are_isolated_in_memory_and_events() {
    let state_a = MockServerState::new();
    state_a.push_text_sse_response(&["Answer for tenant Alpha"]);
    state_a.insert_job(sample_job("job-tenant-a", "Alpha secret: ALPHA-999"));

    let state_b = MockServerState::new();
    state_b.push_text_sse_response(&["Answer for tenant Beta"]);
    state_b.insert_job(sample_job("job-tenant-b", "Beta secret: BETA-888"));

    let (url_a, _ha) = start_mock_server(state_a.clone()).await;
    let (url_b, _hb) = start_mock_server(state_b.clone()).await;

    let cfg_a = sample_worker_config(url_a, "tok-alpha");
    let cfg_b = sample_worker_config(url_b, "tok-beta");

    let (res_a, res_b) = tokio::join!(
        run_single_job(&cfg_a, "job-tenant-a"),
        run_single_job(&cfg_b, "job-tenant-b")
    );

    let out_a = res_a.expect("tenant A succeeds");
    let out_b = res_b.expect("tenant B succeeds");

    assert_eq!(out_a.status, TerminalStatus::Completed);
    assert_eq!(out_b.status, TerminalStatus::Completed);
    assert_eq!(out_a.message.as_deref(), Some("Answer for tenant Alpha"));
    assert_eq!(out_b.message.as_deref(), Some("Answer for tenant Beta"));

    // Confirm zero cross-tenant event leakage.
    let events_a = state_a.events_received.lock().unwrap();
    let events_b = state_b.events_received.lock().unwrap();
    assert!(events_a.iter().all(|e| e.job_id == "job-tenant-a"));
    assert!(events_b.iter().all(|e| e.job_id == "job-tenant-b"));
    assert!(events_a
        .iter()
        .all(|e| !e.data.to_string().contains("Beta")));
    assert!(events_b
        .iter()
        .all(|e| !e.data.to_string().contains("Alpha")));
}
