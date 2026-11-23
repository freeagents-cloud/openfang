mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_worker::billing_policy::{BillingContext, SelectedModelSnapshot};
use openfang_worker::protocol::{TerminalStatus, WorkerEventType};
use openfang_worker::runner::run_single_job;

#[tokio::test]
async fn missing_provider_usage_records_unknown_honestly() {
    let state = MockServerState::new();
    state.push_text_sse_response_without_usage(&["Response without usage frame."]);

    let mut job = sample_job("job-no-usage", "Say hello");
    job.allowed_tools = Vec::new();
    job.billing_context = Some(BillingContext::default());
    job.selected_model = Some(SelectedModelSnapshot::day_one_default());
    state.insert_job(job);

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-no-usage");

    let outcome = run_single_job(&cfg, "job-no-usage")
        .await
        .expect("completes cleanly");
    assert_eq!(outcome.status, TerminalStatus::Completed);

    let events = state.events_received.lock().unwrap();
    let finished = events
        .iter()
        .find(|e| e.event_type == WorkerEventType::ModelCallFinished)
        .expect("model_call_finished event");

    assert_eq!(finished.data["usage_source"], "unknown");
    assert!(finished.data.get("input_tokens").is_none());
    assert!(finished.data.get("output_tokens").is_none());
}
