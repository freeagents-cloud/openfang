mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_bittensor::{
    BittensorModelPolicy, BittensorProxyConfig, BittensorProxyDriver, BudgetLimits,
    DEFAULT_DAY_ONE_MODEL_ID,
};
use openfang_runtime::llm_driver::{CompletionRequest, LlmDriver};
use openfang_types::message::Message;
use openfang_worker::billing_policy::{BillingContext, SelectedModelSnapshot};
use openfang_worker::protocol::{TerminalStatus, WorkerEventType};
use openfang_worker::runner::run_single_job;
use tokio::sync::mpsc;
use zeroize::Zeroizing;

#[tokio::test]
async fn default_day_one_job_executes_through_chutes_with_metering_events() {
    let state = MockServerState::new();
    state.push_text_sse_response(&["Chutes day-one answer."]);

    let mut job = sample_job("job-chutes-default", "Hello Chutes");
    job.allowed_tools = Vec::new(); // capabilities_verified=false forbids tool-needing jobs
    job.billing_context = Some(BillingContext::default());
    job.selected_model = Some(SelectedModelSnapshot::day_one_default());
    state.insert_job(job);

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-chutes-1");

    let outcome = run_single_job(&cfg, "job-chutes-default")
        .await
        .expect("chutes job succeeds");
    assert_eq!(outcome.status, TerminalStatus::Completed);

    // Verify Chutes provider + X-Worker-Model-Call-ID header sent to inference proxy.
    let headers = state.inference_headers.lock().unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(
        headers[0].get("x-bittensor-provider").map(String::as_str),
        Some("chutes")
    );
    let call_id_hdr = headers[0]
        .get("x-worker-model-call-id")
        .expect("x-worker-model-call-id header must be present")
        .clone();
    assert!(call_id_hdr.starts_with("mc-"));

    // Verify model_call_started & model_call_finished events carry matching stable model_call_id.
    let events = state.events_received.lock().unwrap();
    let started = events
        .iter()
        .find(|e| e.event_type == WorkerEventType::ModelCallStarted)
        .expect("model_call_started event");
    assert_eq!(started.data["model_call_id"], call_id_hdr);
    assert_eq!(started.data["provider_requested"], "chutes");
    assert_eq!(started.data["model_requested"], DEFAULT_DAY_ONE_MODEL_ID);

    let finished = events
        .iter()
        .find(|e| e.event_type == WorkerEventType::ModelCallFinished)
        .expect("model_call_finished event");
    assert_eq!(finished.data["model_call_id"], call_id_hdr);
    assert_eq!(finished.data["usage_source"], "provider_stream");
    assert_eq!(finished.data["input_tokens"], 24);
    assert_eq!(finished.data["output_tokens"], 12);

    // Terminal completion records model_call_ids and never embeds customer price.
    let completions = state.completions.lock().unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].provider.as_deref(), Some("chutes"));
    assert_eq!(
        completions[0].model.as_deref(),
        Some(DEFAULT_DAY_ONE_MODEL_ID)
    );
    assert_eq!(completions[0].model_call_ids, vec![call_id_hdr]);
    let raw_complete = serde_json::to_string(&completions[0]).unwrap();
    assert!(!raw_complete.contains("customer_price"));
}

#[tokio::test]
async fn rejects_tool_needing_job_when_capabilities_unverified() {
    let state = MockServerState::new();
    let mut job = sample_job("job-unverified-tools", "Search the web");
    job.allowed_tools = vec!["web_search".into()];
    job.selected_model = Some(SelectedModelSnapshot::day_one_default()); // capabilities_verified=false
    state.insert_job(job);

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-unverified");

    let outcome = run_single_job(&cfg, "job-unverified-tools").await.unwrap();
    assert_eq!(outcome.status, TerminalStatus::Failed);
    assert_eq!(outcome.finish_reason, "policy_rejected");
    assert_eq!(
        state
            .inference_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[tokio::test]
async fn unauthorized_model_cannot_be_smuggled_past_locked_selected_model() {
    let state = MockServerState::new();
    let (server_url, _handle) = start_mock_server(state.clone()).await;

    let policy = BittensorModelPolicy::from_raw(
        &["chutes".into()],
        Some("chutes"),
        BudgetLimits::default(),
    )
    .unwrap();

    let driver = BittensorProxyDriver::new(BittensorProxyConfig {
        server_base_url: server_url,
        job_token: Zeroizing::new("tok-locked".into()),
        job_id: "job-locked".into(),
        attempt: 1,
        trace_id: "trace-locked".into(),
        policy,
        locked_model_id: Some(DEFAULT_DAY_ONE_MODEL_ID.to_string()),
    })
    .unwrap();

    let req = CompletionRequest {
        model: "unauthorized/other-model".into(),
        messages: vec![Message::user("smuggle test")],
        tools: Vec::new(),
        max_tokens: 128,
        temperature: 0.2,
        system: None,
        thinking: None,
    };

    let (tx, _rx) = mpsc::channel(8);
    let err = driver.stream(req, tx).await.unwrap_err();
    assert!(err.to_string().contains("unauthorized model"));
    assert_eq!(
        state
            .inference_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}
