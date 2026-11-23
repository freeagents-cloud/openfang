//! Bittensor-only inference routing, fail-closed model policy, and telemetry
//! for FreeAgents OpenFang workers.
//!
//! This crate enforces the FreeAgents bounty requirement that all LLM inference
//! executes exclusively through approved Bittensor providers (`engy`, `chutes`,
//! `saygm`) via the control-plane Server's scoped inference proxy
//! (`POST /internal/v1/inference/chat`).

pub mod errors;
pub mod metering;
pub mod models;
pub mod policy;
pub mod providers;
pub mod proxy_client;
pub mod telemetry;

pub use errors::BittensorError;
pub use metering::{
    new_model_call_id, ModelCallFinishedEvent, ModelCallStartedEvent, UsageSource,
    DEFAULT_DAY_ONE_MODEL_ID, DEFAULT_DAY_ONE_PRICING_VERSION, DEFAULT_DAY_ONE_PROVIDER,
};
pub use models::{ModelCapabilities, ModelCatalog, ModelDescriptor};
pub use policy::{BittensorModelPolicy, BudgetLimits};
pub use providers::{BittensorProvider, ProviderDescriptor};
pub use proxy_client::{BittensorProxyConfig, BittensorProxyDriver, MeteringCallbacks};
pub use telemetry::{ModelCallRecord, TelemetryCollector};
