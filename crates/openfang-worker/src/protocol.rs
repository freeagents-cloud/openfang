//! Wire schema for the canonical Server `internal-worker.v1` protocol.
//!
//! Authoritative schema owner: `freeagents-cloud/server/openapi/internal-worker.v1.yaml`.

use crate::billing_policy::{
    resolve_billing_policy, BillingContext, ResolvedBillingPolicy, SelectedModelSnapshot,
};
use chrono::{DateTime, Utc};
use openfang_bittensor::{BittensorModelPolicy, ModelCallRecord, DEFAULT_DAY_ONE_MODEL_ID};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Supported protocol version.
pub const PROTOCOL_VERSION: u32 = 1;

/// Job payload returned by `GET /internal/v1/jobs/{job_id}`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobAssignment {
    pub protocol_version: u32,
    pub job_id: String,
    pub attempt: u32,
    pub conversation_id: String,
    pub messages: Vec<JobMessage>,
    pub system_policy_version: String,
    pub model_policy: JobModelPolicy,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
    #[serde(default)]
    pub limits: JobLimits,
    pub trace_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Policy-only billing limits supplied by Server (Appendix A §A.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_context: Option<BillingContext>,
    /// Authoritative model snapshot selected by Server (Appendix A §A.7).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_model: Option<SelectedModelSnapshot>,
}

/// Conversation message supplied by Server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobMessage {
    pub role: String,
    pub content: String,
}

/// Bittensor-only model policy supplied by Server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobModelPolicy {
    pub allowed_providers: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preferred: Option<String>,
}

/// Deterministic execution bounds enforced by the worker (cannot be increased by the worker).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobLimits {
    #[serde(default = "default_max_agent_turns")]
    pub max_agent_turns: u32,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u64,
    #[serde(default = "default_deadline_ms")]
    pub deadline_ms: u64,
    #[serde(default = "default_max_output_bytes")]
    pub max_output_bytes: usize,
}

fn default_max_agent_turns() -> u32 {
    8
}
fn default_max_tokens() -> u64 {
    12_000
}
fn default_deadline_ms() -> u64 {
    90_000
}
fn default_max_output_bytes() -> usize {
    256 * 1024
}

impl Default for JobLimits {
    fn default() -> Self {
        Self {
            max_agent_turns: default_max_agent_turns(),
            max_tokens: default_max_tokens(),
            deadline_ms: default_deadline_ms(),
            max_output_bytes: default_max_output_bytes(),
        }
    }
}

impl JobAssignment {
    /// Validate protocol version, immutable identity fields, billing context,
    /// authoritative `selected_model`, and fail-closed Bittensor policy.
    pub fn validate_and_resolve(&self, fallback_model: &str) -> Result<ResolvedBillingPolicy, String> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(format!(
                "unsupported protocol_version {} (expected {PROTOCOL_VERSION})",
                self.protocol_version
            ));
        }
        if self.job_id.trim().is_empty() {
            return Err("job_id must not be empty".to_string());
        }
        if self.attempt == 0 {
            return Err("attempt must be >= 1".to_string());
        }
        if self.messages.is_empty() {
            return Err("messages must contain at least one user turn".to_string());
        }
        if self.limits.max_agent_turns == 0 {
            return Err("limits.max_agent_turns must be >= 1".to_string());
        }
        if self.limits.deadline_ms == 0 {
            return Err("limits.deadline_ms must be >= 1".to_string());
        }

        let effective_fallback = self
            .model
            .as_deref()
            .filter(|m| !m.trim().is_empty())
            .unwrap_or(if fallback_model.trim().is_empty() {
                DEFAULT_DAY_ONE_MODEL_ID
            } else {
                fallback_model
            });

        resolve_billing_policy(
            &self.model_policy.allowed_providers,
            self.model_policy.preferred.as_deref(),
            self.selected_model.as_ref(),
            self.billing_context.as_ref(),
            &self.allowed_tools,
            self.limits.max_agent_turns,
            self.limits.max_tokens,
            effective_fallback,
        )
    }

    /// Backwards-compatible helper returning only the validated [`BittensorModelPolicy`].
    pub fn validate_and_build_policy(&self) -> Result<BittensorModelPolicy, String> {
        self.validate_and_resolve(DEFAULT_DAY_ONE_MODEL_ID)
            .map(|r| r.model_policy)
    }
}

/// Ordered event envelope sent to `POST /internal/v1/jobs/{job_id}/events`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerEvent {
    pub job_id: String,
    pub attempt: u32,
    pub sequence: u64,
    #[serde(rename = "type")]
    pub event_type: WorkerEventType,
    pub at: DateTime<Utc>,
    pub data: Value,
}

/// Event types defined by Spec §4.3 and Appendix A §A.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkerEventType {
    Started,
    ProviderSelected,
    ModelCallStarted,
    ModelCallFinished,
    TextDelta,
    ToolStarted,
    ToolFinished,
    ToolMetered,
    SourceFound,
    Usage,
    BudgetWarning,
    BudgetExhausted,
    Checkpoint,
    Completed,
    Failed,
    Cancelled,
}

/// Request body for `POST /internal/v1/jobs/{job_id}/events`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBatchRequest {
    pub protocol_version: u32,
    pub job_id: String,
    pub attempt: u32,
    pub trace_id: String,
    pub events: Vec<WorkerEvent>,
}

/// Request body for `POST /internal/v1/jobs/{job_id}/heartbeat`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatRequest {
    pub protocol_version: u32,
    pub job_id: String,
    pub attempt: u32,
    pub trace_id: String,
    pub at: DateTime<Utc>,
}

/// Response body from `POST /internal/v1/jobs/{job_id}/heartbeat`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    #[serde(default)]
    pub cancelled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_expires_at: Option<DateTime<Utc>>,
}

/// Terminal job status reported to `POST /internal/v1/jobs/{job_id}/complete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalStatus {
    Completed,
    Failed,
    Cancelled,
}

/// Token usage summary reported on completion.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct UsageSummary {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub model_calls: u32,
    pub iterations: u32,
}

/// Verified retrieved source citation (only from real tool execution, never fabricated).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    pub source_id: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub retrieved_at: DateTime<Utc>,
    pub tool: String,
}

/// Timing breakdown for a job attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTimings {
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub duration_ms: u64,
}

/// Request body for `POST /internal/v1/jobs/{job_id}/complete`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteJobRequest {
    pub protocol_version: u32,
    pub job_id: String,
    pub attempt: u32,
    pub trace_id: String,
    pub status: TerminalStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    pub usage: UsageSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub finish_reason: String,
    pub timings: JobTimings,
    #[serde(default)]
    pub sources: Vec<SourceRecord>,
    #[serde(default)]
    pub model_calls: Vec<ModelCallRecord>,
    /// Stable model call IDs observed during this attempt (Appendix A §A.3).
    #[serde(default)]
    pub model_call_ids: Vec<String>,
    /// Stable tool call IDs observed during this attempt (Appendix A §A.3).
    #[serde(default)]
    pub tool_call_ids: Vec<String>,
    /// Provider request IDs observed during this attempt (Appendix A §A.3).
    #[serde(default)]
    pub request_ids_seen: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
