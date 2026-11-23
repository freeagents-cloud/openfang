//! Fail-closed error types for Bittensor inference routing and policy enforcement.

use thiserror::Error;

/// Errors produced by Bittensor policy checks or proxy client setup.
#[derive(Debug, Error)]
pub enum BittensorError {
    /// A requested or configured provider is not in the approved Bittensor set.
    #[error("disallowed inference provider '{0}': only engy, chutes, and saygm are permitted")]
    DisallowedProvider(String),

    /// No approved providers were configured in the model policy.
    #[error("model policy must allow at least one Bittensor provider")]
    EmptyProviderList,

    /// The preferred provider is not included in `allowed_providers`.
    #[error("preferred provider '{preferred}' is not in allowed_providers")]
    PreferredNotAllowed { preferred: String },

    /// The requested model is not in the active allowlist or lacks required capabilities.
    #[error("model '{model}' is not permitted for provider '{provider}': {reason}")]
    DisallowedModel {
        provider: String,
        model: String,
        reason: String,
    },

    /// The Server proxy URL is invalid or uses an unsupported scheme.
    #[error("invalid server inference proxy URL '{0}': {1}")]
    InvalidProxyUrl(String, String),

    /// A job-level budget limit (tokens or calls) was exceeded.
    #[error("inference budget exceeded: {0}")]
    BudgetExceeded(String),

    /// A fallback attempt was blocked because output text was already streamed.
    #[error("cannot fail over from '{from}' after partial response text was emitted")]
    FallbackAfterOutput { from: String },
}
