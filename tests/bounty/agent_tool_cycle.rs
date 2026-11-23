mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_worker::protocol::{TerminalStatus, WorkerEventType};
use openfang_worker::runner::run_single_job;

#[tokio::test]
async fn full_agent_loop_with_tool_call_and_grounded_answer() {
    let state = MockServerState::new();

    // Turn 1: model requests a web_fetch tool call to a loopback URL (which SSRF protection
    // safely rejects as a tool error, proving the real OpenFang tool_runner + SSRF guard executed!).
    state.push_tool_call_sse_response(
        "call_ssrf_check",
        "web_fetch",
        r#"{"url":"http://127.0.0.1:9/secret"}"#,
    );
    // Turn 2: model returns grounded final answer after observing tool result.
    state.push_text_sse_response(&["The fetch to loopback was blocked by SSRF policy."]);

    state.insert_job(sample_job(
        "job-tool-cycle-1",
        "Try fetching http://127.0.0.1:9/secret",
    ));

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-tool-cycle");

    let outcome = run_single_job(&cfg, "job-tool-cycle-1")
        .await
        .expect("job completes");

    assert_eq!(outcome.status, TerminalStatus::Completed);
    assert_eq!(
        outcome.message.as_deref(),
        Some("The fetch to loopback was blocked by SSRF policy.")
    );

    let events = state.events_received.lock().unwrap();
    let types: Vec<WorkerEventType> = events.iter().map(|e| e.event_type).collect();
    assert!(types.contains(&WorkerEventType::Started));
    assert!(types.contains(&WorkerEventType::ProviderSelected));
    assert!(types.contains(&WorkerEventType::ToolStarted));
    assert!(types.contains(&WorkerEventType::ToolFinished));
    assert!(types.contains(&WorkerEventType::TextDelta));
    assert!(types.contains(&WorkerEventType::Completed));

    // Verify completion record captured both model calls and usage totals.
    let completions = state.completions.lock().unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, TerminalStatus::Completed);
    assert_eq!(completions[0].usage.model_calls, 2);
    assert_eq!(completions[0].provider.as_deref(), Some("engy"));
    assert_eq!(completions[0].model_calls.len(), 2);
}
