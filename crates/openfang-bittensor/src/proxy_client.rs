//! `LlmDriver` implementation routing all OpenFang model calls through the
//! FreeAgents Server's Bittensor-only inference proxy (`POST /internal/v1/inference/chat`).
//!
//! Reuses upstream [`openfang_runtime::drivers::openai::OpenAIDriver`] for SSE
//! streaming, tool-call delta assembly, think-tag filtering, and usage parsing.
//! Attaches a stable `X-Worker-Model-Call-ID` per logical request (Appendix A §A.4).

use crate::errors::BittensorError;
use crate::metering::{
    new_model_call_id, ModelCallFinishedEvent, ModelCallStartedEvent, UsageSource,
};
use crate::policy::BittensorModelPolicy;
use crate::providers::BittensorProvider;
use crate::telemetry::{ModelCallRecord, TelemetryCollector};
use async_trait::async_trait;
use chrono::Utc;
use openfang_runtime::drivers::openai::OpenAIDriver;
use openfang_runtime::llm_driver::{
    CompletionRequest, CompletionResponse, LlmDriver, LlmError, StreamEvent,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, warn};
use url::Url;
use zeroize::Zeroizing;

/// Configuration for [`BittensorProxyDriver`].
#[derive(Clone)]
pub struct BittensorProxyConfig {
    /// Base URL of the FreeAgents control-plane Server (e.g. `https://api.freeagents.cloud`).
    pub server_base_url: String,
    /// Short-lived job-scoped bearer token issued by Server.
    pub job_token: Zeroizing<String>,
    /// Immutable job identifier.
    pub job_id: String,
    /// Monotonic attempt number for fencing.
    pub attempt: u32,
    /// Opaque distributed trace ID.
    pub trace_id: String,
    /// Fail-closed Bittensor model policy.
    pub policy: BittensorModelPolicy,
    /// Optional authoritative model ID locked by Server's `selected_model` snapshot.
    pub locked_model_id: Option<String>,
}

impl std::fmt::Debug for BittensorProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BittensorProxyConfig")
            .field("server_base_url", &self.server_base_url)
            .field("job_token", &"<redacted>")
            .field("job_id", &self.job_id)
            .field("attempt", &self.attempt)
            .field("trace_id", &self.trace_id)
            .field("policy", &self.policy)
            .field("locked_model_id", &self.locked_model_id)
            .finish()
    }
}

/// Callback invoked whenever a Bittensor provider is selected for a request.
pub type ProviderSelectedCallback = Arc<dyn Fn(BittensorProvider, &str) + Send + Sync>;

/// Callback invoked when a budget cap is reached.
pub type BudgetExhaustedCallback = Arc<dyn Fn(String, u64, u64) + Send + Sync>;

/// Optional callbacks for emitting structured Appendix A metering events.
#[derive(Clone, Default)]
pub struct MeteringCallbacks {
    pub on_started: Option<Arc<dyn Fn(ModelCallStartedEvent) + Send + Sync>>,
    pub on_finished: Option<Arc<dyn Fn(ModelCallFinishedEvent) + Send + Sync>>,
    pub on_budget_exhausted: Option<BudgetExhaustedCallback>,
}

/// Fail-closed `LlmDriver` that wraps upstream `OpenAIDriver` and targets Server's
/// `/internal/v1/inference/chat` proxy endpoint.
pub struct BittensorProxyDriver {
    config: BittensorProxyConfig,
    chat_endpoint: String,
    telemetry: TelemetryCollector,
    on_provider_selected: Option<ProviderSelectedCallback>,
    metering: MeteringCallbacks,
    /// Tracks whether any assistant text delta has already been exposed in this job attempt.
    /// Fallback across providers is forbidden once text has been emitted (Spec §8).
    text_emitted: Arc<AtomicBool>,
    /// Latched to `true` once Server or local budget cap reports `budget_exhausted`.
    /// Prevents issuing another model call under a new ID to bypass caps (Appendix A §A.4.5).
    budget_exhausted: Arc<AtomicBool>,
}

impl BittensorProxyDriver {
    /// Construct and validate a new proxy driver.
    pub fn new(config: BittensorProxyConfig) -> Result<Self, BittensorError> {
        let parsed = Url::parse(&config.server_base_url).map_err(|e| {
            BittensorError::InvalidProxyUrl(config.server_base_url.clone(), e.to_string())
        })?;
        match parsed.scheme() {
            "https" | "http" => {}
            other => {
                return Err(BittensorError::InvalidProxyUrl(
                    config.server_base_url.clone(),
                    format!("unsupported scheme '{other}'"),
                ));
            }
        }

        let trimmed = config.server_base_url.trim_end_matches('/');
        let chat_endpoint = format!("{trimmed}/internal/v1/inference/chat");

        Ok(Self {
            config,
            chat_endpoint,
            telemetry: TelemetryCollector::new(),
            on_provider_selected: None,
            metering: MeteringCallbacks::default(),
            text_emitted: Arc::new(AtomicBool::new(false)),
            budget_exhausted: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Attach a callback notified whenever a provider is chosen for a model call.
    pub fn with_on_provider_selected(mut self, cb: ProviderSelectedCallback) -> Self {
        self.on_provider_selected = Some(cb);
        self
    }

    /// Attach Appendix A metering event callbacks.
    pub fn with_metering_callbacks(mut self, callbacks: MeteringCallbacks) -> Self {
        self.metering = callbacks;
        self
    }

    /// Access the telemetry collector for this driver.
    pub fn telemetry(&self) -> TelemetryCollector {
        self.telemetry.clone()
    }

    /// Returns true if a hard spend/call/token cap was reached during this attempt.
    pub fn is_budget_exhausted(&self) -> bool {
        self.budget_exhausted.load(Ordering::SeqCst)
    }

    /// The resolved Server inference proxy URL.
    pub fn chat_endpoint(&self) -> &str {
        &self.chat_endpoint
    }

    fn build_inner_driver(&self, provider: BittensorProvider, model_call_id: &str) -> OpenAIDriver {
        OpenAIDriver::new(
            self.config.job_token.as_str().to_string(),
            self.config.server_base_url.clone(),
        )
        .with_chat_endpoint(self.chat_endpoint.clone())
        .with_extra_headers(vec![
            (
                "x-bittensor-provider".to_string(),
                provider.as_str().to_string(),
            ),
            ("x-job-id".to_string(), self.config.job_id.clone()),
            (
                "x-job-attempt".to_string(),
                self.config.attempt.to_string(),
            ),
            ("x-trace-id".to_string(), self.config.trace_id.clone()),
            ("x-protocol-version".to_string(), "1".to_string()),
            (
                "x-worker-model-call-id".to_string(),
                model_call_id.to_string(),
            ),
        ])
    }

    fn map_policy_err(err: BittensorError) -> LlmError {
        LlmError::Api {
            status: 403,
            message: err.to_string(),
        }
    }

    fn is_budget_exhausted_error(err: &LlmError) -> bool {
        match err {
            LlmError::Api { status, message } => {
                *status == 402
                    || (*status == 403
                        && (message.to_ascii_lowercase().contains("budget_exhausted")
                            || message.to_ascii_lowercase().contains("budget exceeded")
                            || message.to_ascii_lowercase().contains("spend cap")))
            }
            _ => false,
        }
    }

    fn is_retryable_provider_error(err: &LlmError) -> bool {
        if Self::is_budget_exhausted_error(err) {
            return false;
        }
        matches!(
            err,
            LlmError::Http(_)
                | LlmError::RateLimited { .. }
                | LlmError::Overloaded { .. }
                | LlmError::Api { status: 500..=599, .. }
        )
    }

    fn enforce_locked_model(&self, requested: &str) -> Result<(), LlmError> {
        if let Some(ref locked) = self.config.locked_model_id {
            if requested != locked {
                return Err(LlmError::Api {
                    status: 403,
                    message: format!(
                        "unauthorized model '{requested}': job is locked to Server-selected model '{locked}'"
                    ),
                });
            }
        }
        Ok(())
    }

    fn resolve_usage_source(
        resp: &CompletionResponse,
        is_stream: bool,
    ) -> (UsageSource, Option<u64>, Option<u64>) {
        // Upstream OpenAIDriver synthesizes `(input_tokens=0, output_tokens=1)` when a streaming
        // provider omits the final usage frame; treat that honestly as `UsageSource::Unknown`.
        if resp.usage.input_tokens == 0 && resp.usage.output_tokens <= 1 {
            (UsageSource::Unknown, None, None)
        } else if is_stream {
            (
                UsageSource::ProviderStream,
                Some(resp.usage.input_tokens),
                Some(resp.usage.output_tokens),
            )
        } else {
            (
                UsageSource::ProviderResponse,
                Some(resp.usage.input_tokens),
                Some(resp.usage.output_tokens),
            )
        }
    }
}

#[async_trait]
impl LlmDriver for BittensorProxyDriver {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        if self.budget_exhausted.load(Ordering::SeqCst) {
            return Err(LlmError::Api {
                status: 402,
                message: "budget_exhausted: cannot issue model calls after cap reached".into(),
            });
        }
        self.enforce_locked_model(&request.model)?;

        let candidates = self.config.policy.candidate_order();
        let require_tools = !request.tools.is_empty();
        let mut last_err: Option<LlmError> = None;

        for (idx, provider) in candidates.iter().copied().enumerate() {
            if idx > 0 && self.text_emitted.load(Ordering::SeqCst) {
                return Err(Self::map_policy_err(BittensorError::FallbackAfterOutput {
                    from: candidates[idx - 1].to_string(),
                }));
            }

            if let Err(err) = self.config.policy.check_request(
                provider,
                &request.model,
                false,
                require_tools,
                self.telemetry.call_count(),
                self.telemetry.total_tokens(),
            ) {
                if matches!(err, BittensorError::BudgetExceeded(_)) {
                    self.budget_exhausted.store(true, Ordering::SeqCst);
                    if let Some(ref cb) = self.metering.on_budget_exhausted {
                        cb(
                            "model_calls_or_tokens".into(),
                            self.telemetry.total_tokens(),
                            self.config.policy.budgets().max_tokens,
                        );
                    }
                }
                return Err(Self::map_policy_err(err));
            }

            let model_call_id = new_model_call_id();
            let started_at = Utc::now();

            if let Some(ref cb) = self.on_provider_selected {
                cb(provider, &request.model);
            }
            if let Some(ref cb) = self.metering.on_started {
                cb(ModelCallStartedEvent {
                    model_call_id: model_call_id.clone(),
                    provider_requested: provider,
                    model_requested: request.model.clone(),
                    started_at,
                });
            }

            let inner = self.build_inner_driver(provider, &model_call_id);
            info!(
                model_call_id = %model_call_id,
                provider = %provider,
                model = %request.model,
                job_id = %self.config.job_id,
                attempt = self.config.attempt,
                trace_id = %self.config.trace_id,
                "Dispatching Bittensor inference completion"
            );

            match inner.complete(request.clone()).await {
                Ok(resp) => {
                    let completed_at = Utc::now();
                    if !resp.text().is_empty() {
                        self.text_emitted.store(true, Ordering::SeqCst);
                    }
                    let (usage_source, in_opt, out_opt) = Self::resolve_usage_source(&resp, false);
                    if let Some(ref cb) = self.metering.on_finished {
                        cb(ModelCallFinishedEvent {
                            model_call_id: model_call_id.clone(),
                            provider_actual: provider,
                            model_actual: request.model.clone(),
                            provider_request_id: None,
                            input_tokens: in_opt,
                            output_tokens: out_opt,
                            usage_source,
                            finish_reason: format!("{:?}", resp.stop_reason),
                            completed_at,
                            ambiguous_transport_retry: None,
                        });
                    }
                    self.telemetry.record(ModelCallRecord {
                        model_call_id,
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: completed_at,
                        input_tokens: resp.usage.input_tokens,
                        output_tokens: resp.usage.output_tokens,
                        usage_source,
                        error: None,
                        trace_id: self.config.trace_id.clone(),
                    });
                    return Ok(resp);
                }
                Err(err) => {
                    let completed_at = Utc::now();
                    warn!(
                        model_call_id = %model_call_id,
                        provider = %provider,
                        model = %request.model,
                        error = %err,
                        "Bittensor inference completion failed"
                    );
                    if Self::is_budget_exhausted_error(&err) {
                        self.budget_exhausted.store(true, Ordering::SeqCst);
                        if let Some(ref cb) = self.metering.on_budget_exhausted {
                            cb(
                                "provider_cost_cap".into(),
                                self.telemetry.call_count() as u64,
                                self.config.policy.budgets().max_model_calls as u64,
                            );
                        }
                    }
                    if let Some(ref cb) = self.metering.on_finished {
                        cb(ModelCallFinishedEvent {
                            model_call_id: model_call_id.clone(),
                            provider_actual: provider,
                            model_actual: request.model.clone(),
                            provider_request_id: None,
                            input_tokens: None,
                            output_tokens: None,
                            usage_source: UsageSource::Unknown,
                            finish_reason: "error".into(),
                            completed_at,
                            ambiguous_transport_retry: Some(matches!(err, LlmError::Http(_))),
                        });
                    }
                    self.telemetry.record(ModelCallRecord {
                        model_call_id,
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: completed_at,
                        input_tokens: 0,
                        output_tokens: 0,
                        usage_source: UsageSource::Unknown,
                        error: Some(err.to_string()),
                        trace_id: self.config.trace_id.clone(),
                    });
                    let retryable = Self::is_retryable_provider_error(&err);
                    last_err = Some(err);
                    if !retryable {
                        break;
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| LlmError::Http("no Bittensor provider available".into())))
    }

    async fn stream(
        &self,
        request: CompletionRequest,
        tx: mpsc::Sender<StreamEvent>,
    ) -> Result<CompletionResponse, LlmError> {
        if self.budget_exhausted.load(Ordering::SeqCst) {
            return Err(LlmError::Api {
                status: 402,
                message: "budget_exhausted: cannot issue model calls after cap reached".into(),
            });
        }
        self.enforce_locked_model(&request.model)?;

        let candidates = self.config.policy.candidate_order();
        let require_tools = !request.tools.is_empty();
        let mut last_err: Option<LlmError> = None;

        for (idx, provider) in candidates.iter().copied().enumerate() {
            if idx > 0 && self.text_emitted.load(Ordering::SeqCst) {
                return Err(Self::map_policy_err(BittensorError::FallbackAfterOutput {
                    from: candidates[idx - 1].to_string(),
                }));
            }

            if let Err(err) = self.config.policy.check_request(
                provider,
                &request.model,
                true,
                require_tools,
                self.telemetry.call_count(),
                self.telemetry.total_tokens(),
            ) {
                if matches!(err, BittensorError::BudgetExceeded(_)) {
                    self.budget_exhausted.store(true, Ordering::SeqCst);
                    if let Some(ref cb) = self.metering.on_budget_exhausted {
                        cb(
                            "model_calls_or_tokens".into(),
                            self.telemetry.total_tokens(),
                            self.config.policy.budgets().max_tokens,
                        );
                    }
                }
                return Err(Self::map_policy_err(err));
            }

            let model_call_id = new_model_call_id();
            let started_at = Utc::now();

            if let Some(ref cb) = self.on_provider_selected {
                cb(provider, &request.model);
            }
            if let Some(ref cb) = self.metering.on_started {
                cb(ModelCallStartedEvent {
                    model_call_id: model_call_id.clone(),
                    provider_requested: provider,
                    model_requested: request.model.clone(),
                    started_at,
                });
            }

            let inner = self.build_inner_driver(provider, &model_call_id);
            info!(
                model_call_id = %model_call_id,
                provider = %provider,
                model = %request.model,
                job_id = %self.config.job_id,
                attempt = self.config.attempt,
                trace_id = %self.config.trace_id,
                "Dispatching Bittensor streaming inference"
            );

            let (proxy_tx, mut proxy_rx) = mpsc::channel::<StreamEvent>(64);
            let downstream_tx = tx.clone();
            let text_emitted_flag = Arc::clone(&self.text_emitted);
            let forward_handle = tokio::spawn(async move {
                while let Some(ev) = proxy_rx.recv().await {
                    if let StreamEvent::TextDelta { ref text } = ev {
                        if !text.is_empty() {
                            text_emitted_flag.store(true, Ordering::SeqCst);
                        }
                    }
                    if downstream_tx.send(ev).await.is_err() {
                        break;
                    }
                }
            });

            let result = inner.stream(request.clone(), proxy_tx).await;
            let _ = forward_handle.await;

            match result {
                Ok(resp) => {
                    let completed_at = Utc::now();
                    let (usage_source, in_opt, out_opt) = Self::resolve_usage_source(&resp, true);
                    if let Some(ref cb) = self.metering.on_finished {
                        cb(ModelCallFinishedEvent {
                            model_call_id: model_call_id.clone(),
                            provider_actual: provider,
                            model_actual: request.model.clone(),
                            provider_request_id: None,
                            input_tokens: in_opt,
                            output_tokens: out_opt,
                            usage_source,
                            finish_reason: format!("{:?}", resp.stop_reason),
                            completed_at,
                            ambiguous_transport_retry: None,
                        });
                    }
                    self.telemetry.record(ModelCallRecord {
                        model_call_id,
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: completed_at,
                        input_tokens: resp.usage.input_tokens,
                        output_tokens: resp.usage.output_tokens,
                        usage_source,
                        error: None,
                        trace_id: self.config.trace_id.clone(),
                    });
                    return Ok(resp);
                }
                Err(err) => {
                    let completed_at = Utc::now();
                    warn!(
                        model_call_id = %model_call_id,
                        provider = %provider,
                        model = %request.model,
                        error = %err,
                        "Bittensor streaming inference failed"
                    );
                    if Self::is_budget_exhausted_error(&err) {
                        self.budget_exhausted.store(true, Ordering::SeqCst);
                        if let Some(ref cb) = self.metering.on_budget_exhausted {
                            cb(
                                "provider_cost_cap".into(),
                                self.telemetry.call_count() as u64,
                                self.config.policy.budgets().max_model_calls as u64,
                            );
                        }
                    }
                    if let Some(ref cb) = self.metering.on_finished {
                        cb(ModelCallFinishedEvent {
                            model_call_id: model_call_id.clone(),
                            provider_actual: provider,
                            model_actual: request.model.clone(),
                            provider_request_id: None,
                            input_tokens: None,
                            output_tokens: None,
                            usage_source: UsageSource::Unknown,
                            finish_reason: "error".into(),
                            completed_at,
                            ambiguous_transport_retry: Some(matches!(err, LlmError::Http(_))),
                        });
                    }
                    self.telemetry.record(ModelCallRecord {
                        model_call_id,
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: completed_at,
                        input_tokens: 0,
                        output_tokens: 0,
                        usage_source: UsageSource::Unknown,
                        error: Some(err.to_string()),
                        trace_id: self.config.trace_id.clone(),
                    });
                    let retryable = Self::is_retryable_provider_error(&err);
                    last_err = Some(err);
                    if !retryable || self.text_emitted.load(Ordering::SeqCst) {
                        break;
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| LlmError::Http("no Bittensor provider available".into())))
    }
}
