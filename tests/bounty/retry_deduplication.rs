mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_worker::billing_policy::{BillingContext, SelectedModelSnapshot};
use openfang_worker::protocol::{TerminalStatus, WorkerEventType};
use openfang_worker::runner::run_single_job;
use std::collections::HashSet;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn event_http_retry_does_not_duplicate_model_call_usage() {
    let state = MockServerState::new();
    // Force transient 500 on first /events delivery so the batcher retries the same batch.
    state.fail_events_first_n.store(1, Ordering::SeqCst);
    state.push_text_sse_response(&["Single logical call."]);

    let mut job = sample_job("job-retry-dedupe", "Retry test");
    job.allowed_tools = Vec::new();
    job.billing_context = Some(BillingContext::default());
    job.selected_model = Some(SelectedModelSnapshot::day_one_default());
    state.insert_job(job);

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-retry-dedupe");

    let outcome = run_single_job(&cfg, "job-retry-dedupe")
        .await
        .expect("completes after retry");
    assert_eq!(outcome.status, TerminalStatus::Completed);

    // Exactly one logical model call occurred, with one unique model_call_id.
    assert_eq!(state.inference_call_count.load(Ordering::SeqCst), 1);

    let events = state.events_received.lock().unwrap();
    let finished_ids: Vec<String> = events
        .iter()
        .filter(|e| e.event_type == WorkerEventType::ModelCallFinished)
        .filter_map(|e| e.data["model_call_id"].as_str().map(String::from))
        .collect();
    assert_eq!(finished_ids.len(), 1);

    let unique_seqs: HashSet<u64> = events.iter().map(|e| e.sequence).collect();
    assert_eq!(unique_seqs.len(), events.len());
}
