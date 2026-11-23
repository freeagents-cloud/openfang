//! Fail-closed Bittensor model policy checked at startup and before every model request.

use crate::errors::BittensorError;
use crate::models::ModelCatalog;
use crate::providers::BittensorProvider;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Hard budget limits enforced by the worker before forwarding any model call.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BudgetLimits {
    /// Maximum total tokens (input + output) across the entire job attempt.
    pub max_tokens: u64,
    /// Maximum number of LLM requests allowed in this job attempt.
    pub max_model_calls: u32,
}

impl Default for BudgetLimits {
    fn default() -> Self {
        Self {
            max_tokens: 12_000,
            max_model_calls: 16,
        }
    }
}

/// Fail-closed policy governing which Bittensor providers and models a job may use.
#[derive(Debug, Clone)]
pub struct BittensorModelPolicy {
    allowed_providers: Vec<BittensorProvider>,
    preferred: BittensorProvider,
    catalog: ModelCatalog,
    budgets: BudgetLimits,
}

impl BittensorModelPolicy {
    /// Construct and validate a policy from raw provider names (e.g., from a Server job payload).
    ///
    /// Fails closed if **any** string in `allowed_providers` or `preferred` is not one of
    /// `engy`, `chutes`, or `saygm` (or `"auto"` for `preferred`, which selects the first
    /// allowed Bittensor provider).
    pub fn from_raw(
        allowed_raw: &[String],
        preferred_raw: Option<&str>,
        budgets: BudgetLimits,
    ) -> Result<Self, BittensorError> {
        if allowed_raw.is_empty() {
            return Err(BittensorError::EmptyProviderList);
        }

        let mut allowed_providers = Vec::with_capacity(allowed_raw.len());
        for raw in allowed_raw {
            let provider = BittensorProvider::from_str(raw)?;
            if !allowed_providers.contains(&provider) {
                allowed_providers.push(provider);
            }
        }

        let preferred = match preferred_raw {
            None => allowed_providers[0],
            Some(p) if p.trim().eq_ignore_ascii_case("auto") => allowed_providers[0],
            Some(p) => {
                let parsed = BittensorProvider::from_str(p)?;
                if !allowed_providers.contains(&parsed) {
                    return Err(BittensorError::PreferredNotAllowed {
                        preferred: parsed.to_string(),
                    });
                }
                parsed
            }
        };

        Ok(Self {
            allowed_providers,
            preferred,
            catalog: ModelCatalog::new(),
            budgets,
        })
    }

    /// Attach an explicit verified [`ModelCatalog`] allowlist.
    pub fn with_catalog(mut self, catalog: ModelCatalog) -> Self {
        self.catalog = catalog;
        self
    }

    /// The preferred starting Bittensor provider.
    pub fn preferred(&self) -> BittensorProvider {
        self.preferred
    }

    /// Ordered list of permitted providers, starting with `preferred`.
    pub fn candidate_order(&self) -> Vec<BittensorProvider> {
        let mut order = Vec::with_capacity(self.allowed_providers.len());
        order.push(self.preferred);
        for &p in &self.allowed_providers {
            if p != self.preferred {
                order.push(p);
            }
        }
        order
    }

    /// Configured budget limits.
    pub fn budgets(&self) -> BudgetLimits {
        self.budgets
    }

    /// Pre-call gate executed before every single LLM request.
    pub fn check_request(
        &self,
        provider: BittensorProvider,
        model_id: &str,
        require_streaming: bool,
        require_tools: bool,
        calls_so_far: u32,
        tokens_so_far: u64,
    ) -> Result<(), BittensorError> {
        if !self.allowed_providers.contains(&provider) {
            return Err(BittensorError::DisallowedProvider(provider.to_string()));
        }

        if calls_so_far >= self.budgets.max_model_calls {
            return Err(BittensorError::BudgetExceeded(format!(
                "model call limit reached ({calls_so_far}/{})",
                self.budgets.max_model_calls
            )));
        }

        if tokens_so_far >= self.budgets.max_tokens {
            return Err(BittensorError::BudgetExceeded(format!(
                "token budget reached ({tokens_so_far}/{})",
                self.budgets.max_tokens
            )));
        }

        self.catalog
            .validate(provider, model_id, require_streaming, require_tools)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_centralized_providers() {
        for bad in ["openai", "anthropic", "xai", "gemini", "openrouter", ""] {
            let res = BittensorModelPolicy::from_raw(
                &[bad.to_string()],
                None,
                BudgetLimits::default(),
            );
            assert!(res.is_err(), "expected '{bad}' to be rejected");
        }
    }

    #[test]
    fn accepts_bittensor_providers_and_auto() {
        let policy = BittensorModelPolicy::from_raw(
            &["engy".into(), "chutes".into(), "saygm".into()],
            Some("auto"),
            BudgetLimits::default(),
        )
        .expect("valid policy");
        assert_eq!(policy.preferred(), BittensorProvider::Engy);
        assert_eq!(
            policy.candidate_order(),
            vec![
                BittensorProvider::Engy,
                BittensorProvider::Chutes,
                BittensorProvider::Saygm
            ]
        );
    }

    #[test]
    fn rejects_mixed_allowed_list_containing_non_bittensor() {
        let res = BittensorModelPolicy::from_raw(
            &["engy".into(), "openai".into()],
            Some("engy"),
            BudgetLimits::default(),
        );
        assert!(matches!(res, Err(BittensorError::DisallowedProvider(_))));
    }

    #[test]
    fn enforces_budgets_before_call() {
        let policy = BittensorModelPolicy::from_raw(
            &["engy".into()],
            Some("engy"),
            BudgetLimits {
                max_tokens: 1000,
                max_model_calls: 2,
            },
        )
        .unwrap();

        assert!(policy
            .check_request(BittensorProvider::Engy, "model-a", true, true, 1, 500)
            .is_ok());
        assert!(matches!(
            policy.check_request(BittensorProvider::Engy, "model-a", true, true, 2, 500),
            Err(BittensorError::BudgetExceeded(_))
        ));
        assert!(matches!(
            policy.check_request(BittensorProvider::Engy, "model-a", true, true, 1, 1000),
            Err(BittensorError::BudgetExceeded(_))
        ));
    }
}
