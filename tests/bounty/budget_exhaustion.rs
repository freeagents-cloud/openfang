mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_bittensor::{
    BittensorModelPolicy, BittensorProxyConfig, BittensorProxyDriver, BudgetLimits,
};
use openfang_runtime::llm_driver::{CompletionRequest, LlmDriver};
use openfang_types::message::Message;
use openfang_worker::billing_policy::{BillingContext, SelectedModelSnapshot};
use openfang_worker::protocol::{JobAssignment, TerminalStatus, WorkerEventType};
use openfang_worker::runner::run_single_job;
use serde_json::json;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use zeroize::Zeroizing;

#[tokio::test]
async fn server_budget_exhausted_aborts_loop_and_blocks_further_calls() {
    let state = MockServerState::new();
    state.force_budget_exhausted.store(true, Ordering::SeqCst);

    let mut job = sample_job("job-budget-cap", "Do expensive work");
    job.allowed_tools = Vec::new();
    job.billing_context = Some(BillingContext::default());
    job.selected_model = Some(SelectedModelSnapshot::day_one_default());
    state.insert_job(job);

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url.clone(), "tok-cap");

    let outcome = run_single_job(&cfg, "job-budget-cap")
        .await
        .expect("returns bounded terminal outcome");
    assert!(matches!(
        outcome.status,
        TerminalStatus::Cancelled | TerminalStatus::Failed
    ));

    {
        let events = state.events_received.lock().unwrap();
        assert!(events
            .iter()
            .any(|e| e.event_type == WorkerEventType::BudgetExhausted));
    }

    // Also verify driver directly latches closed and refuses a second call after 402.
    let policy = BittensorModelPolicy::from_raw(
        &["chutes".into()],
        Some("chutes"),
        BudgetLimits::default(),
    )
    .unwrap();
    let driver = BittensorProxyDriver::new(BittensorProxyConfig {
        server_base_url: server_url,
        job_token: Zeroizing::new("tok-cap-2".into()),
        job_id: "job-cap-2".into(),
        attempt: 1,
        trace_id: "trace-cap-2".into(),
        policy,
        locked_model_id: None,
    })
    .unwrap();

    let req = CompletionRequest {
        model: "deepseek-ai/DeepSeek-V4.1-Flash-1017".into(),
        messages: vec![Message::user("first")],
        tools: Vec::new(),
        max_tokens: 64,
        temperature: 0.1,
        system: None,
        thinking: None,
    };
    let (tx1, _rx1) = mpsc::channel(4);
    assert!(driver.stream(req.clone(), tx1).await.is_err());
    assert!(driver.is_budget_exhausted());

    // Clear server 402 flag — driver MUST still refuse locally because cap latched!
    state.force_budget_exhausted.store(false, Ordering::SeqCst);
    let before = state.inference_headers.lock().unwrap().len();
    let (tx2, _rx2) = mpsc::channel(4);
    let err2 = driver.stream(req, tx2).await.unwrap_err();
    assert!(err2.to_string().contains("budget_exhausted"));
    assert_eq!(state.inference_headers.lock().unwrap().len(), before);
}

#[test]
fn purported_free_flag_has_zero_effect_on_cost_enforcement() {
    let raw = json!({
        "protocol_version": 1,
        "job_id": "job-free-spoof",
        "attempt": 1,
        "conversation_id": "conv-1",
        "messages": [{"role": "user", "content": "hi"}],
        "system_policy_version": "bounty-v1",
        "model_policy": {"allowed_providers": ["chutes"], "preferred": "chutes"},
        "allowed_tools": [],
        "limits": {"max_agent_turns": 4, "max_tokens": 1000, "deadline_ms": 10000},
        "trace_id": "tr-1",
        "free": true,
        "billing_context": {
            "cost_policy_id": "cost-plus-15-v1",
            "max_provider_cost_usd_micro": 0,
            "max_model_calls": 2,
            "max_tool_calls": 0,
            "metering_required": true
        }
    });

    // Deserializing ignores unknown `free: true`, and zero cost cap is still rejected fail-closed.
    let job: JobAssignment = serde_json::from_value(raw).expect("deserializes");
    let err = job.validate_and_resolve("default").unwrap_err();
    assert!(err.contains("max_provider_cost_usd_micro"));
}
