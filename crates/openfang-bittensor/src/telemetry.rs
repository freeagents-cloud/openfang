//! Per-model-call provenance and telemetry records.

use crate::metering::UsageSource;
use crate::providers::BittensorProvider;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// Provenance record captured for every LLM call made through the Bittensor proxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelCallRecord {
    pub model_call_id: String,
    pub provider: BittensorProvider,
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider_request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub inference_proof: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub usage_source: UsageSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub trace_id: String,
}

/// Thread-safe collector of model call records and cumulative token/call totals for one job attempt.
#[derive(Debug, Default, Clone)]
pub struct TelemetryCollector {
    inner: Arc<Mutex<TelemetryState>>,
}

#[derive(Debug, Default)]
struct TelemetryState {
    records: Vec<ModelCallRecord>,
    total_input_tokens: u64,
    total_output_tokens: u64,
}

impl TelemetryCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&self, rec: ModelCallRecord) {
        let mut guard = self.inner.lock().expect("telemetry mutex poisoned");
        guard.total_input_tokens = guard.total_input_tokens.saturating_add(rec.input_tokens);
        guard.total_output_tokens = guard.total_output_tokens.saturating_add(rec.output_tokens);
        guard.records.push(rec);
    }

    pub fn call_count(&self) -> u32 {
        let guard = self.inner.lock().expect("telemetry mutex poisoned");
        guard.records.len() as u32
    }

    pub fn total_tokens(&self) -> u64 {
        let guard = self.inner.lock().expect("telemetry mutex poisoned");
        guard
            .total_input_tokens
            .saturating_add(guard.total_output_tokens)
    }

    pub fn totals(&self) -> (u64, u64) {
        let guard = self.inner.lock().expect("telemetry mutex poisoned");
        (guard.total_input_tokens, guard.total_output_tokens)
    }

    pub fn snapshot(&self) -> Vec<ModelCallRecord> {
        let guard = self.inner.lock().expect("telemetry mutex poisoned");
        guard.records.clone()
    }
}
