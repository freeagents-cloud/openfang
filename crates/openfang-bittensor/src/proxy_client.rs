//! `LlmDriver` implementation routing all OpenFang model calls through the
//! FreeAgents Server's Bittensor-only inference proxy (`POST /internal/v1/inference/chat`).
//!
//! Reuses upstream [`openfang_runtime::drivers::openai::OpenAIDriver`] for SSE
//! streaming, tool-call delta assembly, think-tag filtering, and usage parsing.

use crate::errors::BittensorError;
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
            .finish()
    }
}

/// Callback invoked whenever a Bittensor provider is selected for a request.
pub type ProviderSelectedCallback = Arc<dyn Fn(BittensorProvider, &str) + Send + Sync>;

/// Fail-closed `LlmDriver` that wraps upstream `OpenAIDriver` and targets Server's
/// `/internal/v1/inference/chat` proxy endpoint.
pub struct BittensorProxyDriver {
    config: BittensorProxyConfig,
    chat_endpoint: String,
    telemetry: TelemetryCollector,
    on_provider_selected: Option<ProviderSelectedCallback>,
    /// Tracks whether any assistant text delta has already been exposed in this job attempt.
    /// Fallback across providers is forbidden once text has been emitted (Spec §8).
    text_emitted: Arc<AtomicBool>,
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
            text_emitted: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Attach a callback notified whenever a provider is chosen for a model call.
    pub fn with_on_provider_selected(mut self, cb: ProviderSelectedCallback) -> Self {
        self.on_provider_selected = Some(cb);
        self
    }

    /// Access the telemetry collector for this driver.
    pub fn telemetry(&self) -> TelemetryCollector {
        self.telemetry.clone()
    }

    /// The resolved Server inference proxy URL.
    pub fn chat_endpoint(&self) -> &str {
        &self.chat_endpoint
    }

    fn build_inner_driver(&self, provider: BittensorProvider) -> OpenAIDriver {
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
        ])
    }

    fn map_policy_err(err: BittensorError) -> LlmError {
        LlmError::Api {
            status: 403,
            message: err.to_string(),
        }
    }

    fn is_retryable_provider_error(err: &LlmError) -> bool {
        matches!(
            err,
            LlmError::Http(_)
                | LlmError::RateLimited { .. }
                | LlmError::Overloaded { .. }
                | LlmError::Api { status: 500..=599, .. }
        )
    }
}

#[async_trait]
impl LlmDriver for BittensorProxyDriver {
    async fn complete(&self, request: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        let candidates = self.config.policy.candidate_order();
        let require_tools = !request.tools.is_empty();
        let mut last_err: Option<LlmError> = None;

        for (idx, provider) in candidates.iter().copied().enumerate() {
            if idx > 0 && self.text_emitted.load(Ordering::SeqCst) {
                return Err(Self::map_policy_err(BittensorError::FallbackAfterOutput {
                    from: candidates[idx - 1].to_string(),
                }));
            }

            self.config
                .policy
                .check_request(
                    provider,
                    &request.model,
                    false,
                    require_tools,
                    self.telemetry.call_count(),
                    self.telemetry.total_tokens(),
                )
                .map_err(Self::map_policy_err)?;

            if let Some(ref cb) = self.on_provider_selected {
                cb(provider, &request.model);
            }

            let started_at = Utc::now();
            let inner = self.build_inner_driver(provider);
            info!(
                provider = %provider,
                model = %request.model,
                job_id = %self.config.job_id,
                attempt = self.config.attempt,
                trace_id = %self.config.trace_id,
                "Dispatching Bittensor inference completion"
            );

            match inner.complete(request.clone()).await {
                Ok(resp) => {
                    if !resp.text().is_empty() {
                        self.text_emitted.store(true, Ordering::SeqCst);
                    }
                    self.telemetry.record(ModelCallRecord {
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: Utc::now(),
                        input_tokens: resp.usage.input_tokens,
                        output_tokens: resp.usage.output_tokens,
                        error: None,
                        trace_id: self.config.trace_id.clone(),
                    });
                    return Ok(resp);
                }
                Err(err) => {
                    warn!(
                        provider = %provider,
                        model = %request.model,
                        error = %err,
                        "Bittensor inference completion failed"
                    );
                    self.telemetry.record(ModelCallRecord {
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: Utc::now(),
                        input_tokens: 0,
                        output_tokens: 0,
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
        let candidates = self.config.policy.candidate_order();
        let require_tools = !request.tools.is_empty();
        let mut last_err: Option<LlmError> = None;

        for (idx, provider) in candidates.iter().copied().enumerate() {
            if idx > 0 && self.text_emitted.load(Ordering::SeqCst) {
                return Err(Self::map_policy_err(BittensorError::FallbackAfterOutput {
                    from: candidates[idx - 1].to_string(),
                }));
            }

            self.config
                .policy
                .check_request(
                    provider,
                    &request.model,
                    true,
                    require_tools,
                    self.telemetry.call_count(),
                    self.telemetry.total_tokens(),
                )
                .map_err(Self::map_policy_err)?;

            if let Some(ref cb) = self.on_provider_selected {
                cb(provider, &request.model);
            }

            let started_at = Utc::now();
            let inner = self.build_inner_driver(provider);
            info!(
                provider = %provider,
                model = %request.model,
                job_id = %self.config.job_id,
                attempt = self.config.attempt,
                trace_id = %self.config.trace_id,
                "Dispatching Bittensor streaming inference"
            );

            // Intercept stream events to detect whether any assistant text has been exposed.
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
                    self.telemetry.record(ModelCallRecord {
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: Utc::now(),
                        input_tokens: resp.usage.input_tokens,
                        output_tokens: resp.usage.output_tokens,
                        error: None,
                        trace_id: self.config.trace_id.clone(),
                    });
                    return Ok(resp);
                }
                Err(err) => {
                    warn!(
                        provider = %provider,
                        model = %request.model,
                        error = %err,
                        "Bittensor streaming inference failed"
                    );
                    self.telemetry.record(ModelCallRecord {
                        provider,
                        model_id: request.model.clone(),
                        provider_request_id: None,
                        inference_proof: None,
                        started_at,
                        finished_at: Utc::now(),
                        input_tokens: 0,
                        output_tokens: 0,
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
