//! Allowlisted Bittensor model descriptors and capability flags.
//!
//! Models are explicitly registered or discovered via capability probes rather
//! than guessed; unsupported tool-calling or streaming combinations fail closed.

use crate::errors::BittensorError;
use crate::providers::BittensorProvider;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Capability flags verified for a provider+model pair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelCapabilities {
    pub streaming: bool,
    pub tool_calling: bool,
    pub max_context_tokens: u32,
}

impl Default for ModelCapabilities {
    fn default() -> Self {
        Self {
            streaming: true,
            tool_calling: true,
            max_context_tokens: 128_000,
        }
    }
}

/// Descriptor for a single allowlisted model on a Bittensor provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub provider: BittensorProvider,
    pub model_id: String,
    pub capabilities: ModelCapabilities,
}

/// Allowlist of verified models per Bittensor provider.
#[derive(Debug, Clone, Default)]
pub struct ModelCatalog {
    /// Keyed by `(provider, model_id)`.
    entries: HashMap<(BittensorProvider, String), ModelDescriptor>,
}

impl ModelCatalog {
    /// Create an empty catalog (when empty, any model ID approved by the Server
    /// job policy is permitted, while non-Bittensor providers remain blocked).
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a verified model descriptor.
    pub fn register(&mut self, descriptor: ModelDescriptor) {
        self.entries
            .insert((descriptor.provider, descriptor.model_id.clone()), descriptor);
    }

    /// Returns true if the catalog has explicit allowlist entries.
    pub fn is_enforced(&self) -> bool {
        !self.entries.is_empty()
    }

    /// Validate that `model_id` is allowed on `provider` and satisfies required capabilities.
    pub fn validate(
        &self,
        provider: BittensorProvider,
        model_id: &str,
        require_streaming: bool,
        require_tools: bool,
    ) -> Result<(), BittensorError> {
        if model_id.trim().is_empty() {
            return Err(BittensorError::DisallowedModel {
                provider: provider.to_string(),
                model: model_id.to_string(),
                reason: "model identifier is empty".to_string(),
            });
        }

        if !self.is_enforced() {
            return Ok(());
        }

        let Some(desc) = self.entries.get(&(provider, model_id.to_string())) else {
            return Err(BittensorError::DisallowedModel {
                provider: provider.to_string(),
                model: model_id.to_string(),
                reason: "not present in verified Bittensor model allowlist".to_string(),
            });
        };

        if require_streaming && !desc.capabilities.streaming {
            return Err(BittensorError::DisallowedModel {
                provider: provider.to_string(),
                model: model_id.to_string(),
                reason: "model does not support streaming".to_string(),
            });
        }

        if require_tools && !desc.capabilities.tool_calling {
            return Err(BittensorError::DisallowedModel {
                provider: provider.to_string(),
                model: model_id.to_string(),
                reason: "model does not support tool calling".to_string(),
            });
        }

        Ok(())
    }
}
