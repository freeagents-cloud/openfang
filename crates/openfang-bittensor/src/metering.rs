//! Trustworthy usage measurement envelopes for paid jobs (Appendix A).
//!
//! Explicitly separates **observed usage** (worker-side diagnostics) from
//! **provider invoiced charges** (Server authoritative). Never calculates customer
//! prices or assumes provider unit rates.

use crate::providers::BittensorProvider;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Day-one default provider and model snapshot (Appendix A §A.7).
pub const DEFAULT_DAY_ONE_PROVIDER: BittensorProvider = BittensorProvider::Chutes;
pub const DEFAULT_DAY_ONE_MODEL_ID: &str = "deepseek-ai/DeepSeek-V4.1-Flash-1017";
pub const DEFAULT_DAY_ONE_PRICING_VERSION: &str = "chutes-catalog-observed-2026-11-22";

/// Generate a stable, globally unique identifier for a single logical model call.
pub fn new_model_call_id() -> String {
    format!("mc-{}", Uuid::new_v4())
}

/// Honest provenance of token counts observed by the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageSource {
    /// Parsed from a final streamed SSE usage frame.
    ProviderStream,
    /// Parsed from a non-streaming provider JSON response.
    ProviderResponse,
    /// Provider did not supply usage counts; Server must reconcile independently.
    Unknown,
}

/// Emitted immediately before forwarding a model request to the Server proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCallStartedEvent {
    pub model_call_id: String,
    pub provider_requested: BittensorProvider,
    pub model_requested: String,
    pub started_at: DateTime<Utc>,
}

/// Emitted when a model request finishes (or fails) on the Server proxy.
///
/// Server's own proxy usage/cost records remain authoritative for billing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCallFinishedEvent {
    pub model_call_id: String,
    pub provider_actual: BittensorProvider,
    pub model_actual: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    pub usage_source: UsageSource,
    pub finish_reason: String,
    pub completed_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ambiguous_transport_retry: Option<bool>,
}
