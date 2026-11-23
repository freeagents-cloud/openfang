mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_worker::protocol::{TerminalStatus, WorkerEventType};
use openfang_worker::runner::run_single_job;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn event_ordering_coalescing_and_retry_on_transient_5xx() {
    let state = MockServerState::new();
    // Inject transient 500s on both /events and /complete to verify retry safety.
    state.fail_events_first_n.store(1, Ordering::SeqCst);
    state.fail_complete_first_n.store(1, Ordering::SeqCst);
    state.push_text_sse_response(&["One ", "two ", "three ", "four."]);
    state.insert_job(sample_job("job-proto-1", "Count to four"));

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-proto-1");

    let outcome = run_single_job(&cfg, "job-proto-1")
        .await
        .expect("job completes despite transient 5xx");

    assert_eq!(outcome.status, TerminalStatus::Completed);
    assert_eq!(outcome.message.as_deref(), Some("One two three four."));

    // Completion succeeded after retry.
    let completions = state.completions.lock().unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, TerminalStatus::Completed);

    // Sequences are strictly increasing.
    let events = state.events_received.lock().unwrap();
    assert!(!events.is_empty());
    for pair in events.windows(2) {
        assert!(
            pair[1].sequence > pair[0].sequence,
            "events must be strictly ordered: {} <= {}",
            pair[1].sequence,
            pair[0].sequence
        );
    }

    // Reconstructed text from coalesced text_delta events matches final answer.
    let streamed_text: String = events
        .iter()
        .filter(|e| e.event_type == WorkerEventType::TextDelta)
        .filter_map(|e| e.data.get("delta").and_then(|v| v.as_str()))
        .collect();
    assert_eq!(streamed_text, "One two three four.");
}
