mod common;

use common::{sample_job, sample_worker_config, start_mock_server, MockServerState};
use openfang_bittensor::{
    BittensorError, BittensorModelPolicy, BittensorProvider, BudgetLimits, ModelCapabilities,
    ModelCatalog, ModelDescriptor,
};
use openfang_worker::protocol::TerminalStatus;
use openfang_worker::runner::run_single_job;

#[tokio::test]
async fn worker_rejects_job_with_centralized_provider_fail_closed() {
    let state = MockServerState::new();
    let mut job = sample_job("job-bad-provider", "hello");
    job.model_policy.allowed_providers = vec!["engy".into(), "openai".into()];
    state.insert_job(job);

    let (server_url, _handle) = start_mock_server(state.clone()).await;
    let cfg = sample_worker_config(server_url, "tok-1");

    let outcome = run_single_job(&cfg, "job-bad-provider")
        .await
        .expect("worker returns policy_rejected outcome");

    assert_eq!(outcome.status, TerminalStatus::Failed);
    assert_eq!(outcome.finish_reason, "policy_rejected");

    // Zero inference calls were attempted.
    assert_eq!(
        state
            .inference_call_count
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );

    let completions = state.completions.lock().unwrap();
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].status, TerminalStatus::Failed);
    assert!(completions[0]
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("disallowed inference provider"));
}

#[test]
fn model_catalog_fails_closed_on_unlisted_or_incapable_models() {
    let mut catalog = ModelCatalog::new();
    catalog.register(ModelDescriptor {
        provider: BittensorProvider::Engy,
        model_id: "engy-chat-no-tools".into(),
        capabilities: ModelCapabilities {
            streaming: true,
            tool_calling: false,
            max_context_tokens: 32_000,
        },
    });

    let policy = BittensorModelPolicy::from_raw(
        &["engy".into()],
        Some("engy"),
        BudgetLimits::default(),
    )
    .unwrap()
    .with_catalog(catalog);

    // Allowed without tools
    assert!(policy
        .check_request(BittensorProvider::Engy, "engy-chat-no-tools", true, false, 0, 0)
        .is_ok());

    // Rejected when tools are required
    assert!(matches!(
        policy.check_request(BittensorProvider::Engy, "engy-chat-no-tools", true, true, 0, 0),
        Err(BittensorError::DisallowedModel { .. })
    ));

    // Rejected when model is not in allowlist
    assert!(matches!(
        policy.check_request(BittensorProvider::Engy, "gpt-4o", true, false, 0, 0),
        Err(BittensorError::DisallowedModel { .. })
    ));
}
