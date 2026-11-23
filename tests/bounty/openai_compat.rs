mod common;

use common::{start_mock_server, MockServerState};
use openfang_bittensor::{
    BittensorModelPolicy, BittensorProvider, BittensorProxyConfig, BittensorProxyDriver,
    BudgetLimits,
};
use openfang_runtime::llm_driver::{CompletionRequest, LlmDriver, StreamEvent};
use openfang_types::message::Message;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use zeroize::Zeroizing;

#[tokio::test]
async fn streaming_preserves_text_usage_and_bittensor_headers() {
    let state = MockServerState::new();
    state.push_text_sse_response(&["Hello ", "from ", "Engy!"]);
    let (server_url, _handle) = start_mock_server(state.clone()).await;

    let policy = BittensorModelPolicy::from_raw(
        &["engy".into(), "chutes".into()],
        Some("engy"),
        BudgetLimits::default(),
    )
    .unwrap();

    let driver = BittensorProxyDriver::new(BittensorProxyConfig {
        server_base_url: server_url,
        job_token: Zeroizing::new("job-token-123".into()),
        job_id: "job-stream-1".into(),
        attempt: 2,
        trace_id: "trace-abc".into(),
        policy,
        locked_model_id: None,
    })
    .unwrap();

    let req = CompletionRequest {
        model: "bittensor-test-model".into(),
        messages: vec![Message::user("hi")],
        tools: Vec::new(),
        max_tokens: 256,
        temperature: 0.2,
        system: Some("You are helpful.".into()),
        thinking: None,
    };

    let (tx, mut rx) = mpsc::channel(32);
    let resp = driver.stream(req, tx).await.expect("streaming succeeds");

    let mut deltas = String::new();
    while let Some(ev) = rx.recv().await {
        if let StreamEvent::TextDelta { text } = ev {
            deltas.push_str(&text);
        }
    }

    assert_eq!(deltas, "Hello from Engy!");
    assert_eq!(resp.text(), "Hello from Engy!");
    assert_eq!(resp.usage.input_tokens, 24);
    assert_eq!(resp.usage.output_tokens, 12);

    let headers = state.inference_headers.lock().unwrap();
    assert_eq!(headers.len(), 1);
    assert_eq!(headers[0].get("x-bittensor-provider").map(String::as_str), Some("engy"));
    assert_eq!(headers[0].get("x-job-id").map(String::as_str), Some("job-stream-1"));
    assert_eq!(headers[0].get("x-job-attempt").map(String::as_str), Some("2"));
    assert_eq!(headers[0].get("x-trace-id").map(String::as_str), Some("trace-abc"));

    let telemetry = driver.telemetry().snapshot();
    assert_eq!(telemetry.len(), 1);
    assert_eq!(telemetry[0].provider, BittensorProvider::Engy);
    assert_eq!(telemetry[0].input_tokens, 24);
    assert_eq!(telemetry[0].output_tokens, 12);
}

#[tokio::test]
async fn fails_over_engy_to_chutes_before_output_emitted() {
    let state = MockServerState::new();
    state.fail_inference_first_n.store(1, Ordering::SeqCst);
    state.push_text_sse_response(&["Recovered via Chutes"]);
    let (server_url, _handle) = start_mock_server(state.clone()).await;

    let policy = BittensorModelPolicy::from_raw(
        &["engy".into(), "chutes".into()],
        Some("engy"),
        BudgetLimits::default(),
    )
    .unwrap();

    let driver = BittensorProxyDriver::new(BittensorProxyConfig {
        server_base_url: server_url,
        job_token: Zeroizing::new("job-token-fallback".into()),
        job_id: "job-fallback-1".into(),
        attempt: 1,
        trace_id: "trace-fb".into(),
        policy,
        locked_model_id: None,
    })
    .unwrap();

    let req = CompletionRequest {
        model: "bittensor-test-model".into(),
        messages: vec![Message::user("hello")],
        tools: Vec::new(),
        max_tokens: 128,
        temperature: 0.1,
        system: None,
        thinking: None,
    };

    let (tx, _rx) = mpsc::channel(16);
    let resp = driver.stream(req, tx).await.expect("fallback succeeds");
    assert_eq!(resp.text(), "Recovered via Chutes");

    let headers = state.inference_headers.lock().unwrap();
    assert_eq!(headers.len(), 2);
    assert_eq!(headers[0].get("x-bittensor-provider").map(String::as_str), Some("engy"));
    assert_eq!(headers[1].get("x-bittensor-provider").map(String::as_str), Some("chutes"));

    let telemetry = driver.telemetry().snapshot();
    assert_eq!(telemetry.len(), 2);
    assert!(telemetry[0].error.is_some());
    assert_eq!(telemetry[1].provider, BittensorProvider::Chutes);
    assert!(telemetry[1].error.is_none());
}
