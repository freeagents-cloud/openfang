//! Job-scoped billing & model-selection policy enforcement (Appendix A).
//!
//! Policy-only: the worker NEVER calculates customer prices, NEVER holds balances,
//! Stripe secrets, or wallet keys, and may only tighten (never widen) Server limits.
//! Any client-supplied `free=true` flag is ignored completely.

use openfang_bittensor::{
    BittensorModelPolicy, BittensorProvider, BudgetLimits, DEFAULT_DAY_ONE_MODEL_ID,
    DEFAULT_DAY_ONE_PRICING_VERSION, DEFAULT_DAY_ONE_PROVIDER,
};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Non-secret, policy-only billing bounds supplied by Server in `JobAssignment`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BillingContext {
    pub cost_policy_id: String,
    pub max_provider_cost_usd_micro: u64,
    pub max_model_calls: u32,
    pub max_tool_calls: u32,
    #[serde(default = "default_metering_required")]
    pub metering_required: bool,
}

fn default_metering_required() -> bool {
    true
}

impl Default for BillingContext {
    fn default() -> Self {
        Self {
            cost_policy_id: "cost-plus-15-v1".to_string(),
            max_provider_cost_usd_micro: 2_000_000, // $2.00 ceiling default
            max_model_calls: 10,
            max_tool_calls: 6,
            metering_required: true,
        }
    }
}

/// Authoritative model snapshot selected by Server from its allowlisted catalog (§A.7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedModelSnapshot {
    pub provider: String,
    pub model_id: String,
    pub pricing_version: String,
    #[serde(default)]
    pub capabilities_verified: bool,
}

impl SelectedModelSnapshot {
    /// Day-one default Chutes snapshot per Appendix A §A.7.
    pub fn day_one_default() -> Self {
        Self {
            provider: DEFAULT_DAY_ONE_PROVIDER.as_str().to_string(),
            model_id: DEFAULT_DAY_ONE_MODEL_ID.to_string(),
            pricing_version: DEFAULT_DAY_ONE_PRICING_VERSION.to_string(),
            capabilities_verified: false,
        }
    }
}

/// Validated effective policy resolved from `JobAssignment` + `BillingContext` + `SelectedModelSnapshot`.
#[derive(Debug, Clone)]
pub struct ResolvedBillingPolicy {
    pub model_policy: BittensorModelPolicy,
    pub selected_provider: BittensorProvider,
    pub selected_model_id: String,
    pub max_model_calls: u32,
    pub max_tool_calls: u32,
    pub max_provider_cost_usd_micro: u64,
    pub metering_required: bool,
}

/// Validate `billing_context`, `selected_model`, and `allowed_tools` fail-closed.
#[allow(clippy::too_many_arguments)]
pub fn resolve_billing_policy(
    allowed_providers: &[String],
    preferred_provider: Option<&str>,
    selected_model: Option<&SelectedModelSnapshot>,
    billing_context: Option<&BillingContext>,
    allowed_tools: &[String],
    max_agent_turns: u32,
    max_tokens: u64,
    fallback_model_id: &str,
) -> Result<ResolvedBillingPolicy, String> {
    let billing = billing_context.cloned().unwrap_or_default();
    if billing.cost_policy_id.trim().is_empty() {
        return Err("billing_context.cost_policy_id must not be empty".into());
    }
    if billing.max_model_calls == 0 {
        return Err("billing_context.max_model_calls must be >= 1".into());
    }
    if billing.max_provider_cost_usd_micro == 0 {
        return Err("billing_context.max_provider_cost_usd_micro must be >= 1".into());
    }

    // Tighten model calls cap: never expand beyond either limits or billing_context.
    let effective_max_model_calls = billing
        .max_model_calls
        .min(max_agent_turns.saturating_mul(2).max(1));

    let budgets = BudgetLimits {
        max_tokens,
        max_model_calls: effective_max_model_calls,
    };

    // Determine effective provider & model ID from authoritative `selected_model` when provided.
    let (effective_preferred, selected_model_id, capabilities_verified) =
        if let Some(sm) = selected_model {
            if sm.model_id.trim().is_empty() {
                return Err("selected_model.model_id must not be empty".into());
            }
            if sm.pricing_version.trim().is_empty() {
                return Err("selected_model.pricing_version must not be empty".into());
            }
            // Validate selected_model.provider is an allowed Bittensor provider.
            let _ = BittensorProvider::from_str(&sm.provider).map_err(|e| e.to_string())?;
            (
                Some(sm.provider.as_str()),
                sm.model_id.clone(),
                sm.capabilities_verified,
            )
        } else {
            (preferred_provider, fallback_model_id.to_string(), true)
        };

    // §A.7: Reject tool-needing jobs when capabilities_verified is explicitly false.
    if selected_model.is_some() && !capabilities_verified && !allowed_tools.is_empty() {
        return Err(format!(
            "selected model '{}' has capabilities_verified=false; tool-needing jobs are rejected until compatibility is verified",
            selected_model_id
        ));
    }

    let model_policy =
        BittensorModelPolicy::from_raw(allowed_providers, effective_preferred, budgets)
            .map_err(|e| e.to_string())?;

    let selected_provider = model_policy.preferred();

    Ok(ResolvedBillingPolicy {
        model_policy,
        selected_provider,
        selected_model_id,
        max_model_calls: effective_max_model_calls,
        max_tool_calls: billing.max_tool_calls,
        max_provider_cost_usd_micro: billing.max_provider_cost_usd_micro,
        metering_required: billing.metering_required,
    })
}

/// Environment variable names that must NEVER exist inside a paid worker sandbox (§A.6).
pub const FORBIDDEN_SANDBOX_ENV_VARS: &[&str] = &[
    "STRIPE_SECRET_KEY",
    "STRIPE_WEBHOOK_SECRET",
    "TAO_PRIVATE_KEY",
    "BITTENSOR_WALLET_MNEMONIC",
    "DATABASE_URL",
    "APP_DATABASE_URL",
    "CATHEDRAL_API_KEY",
    "ENGY_API_KEY",
    "CHUTES_API_KEY",
    "SAYGM_API_KEY",
];

/// Audit the current process environment and return any forbidden billing/database/provider secrets found.
pub fn detect_forbidden_sandbox_env() -> Vec<&'static str> {
    FORBIDDEN_SANDBOX_ENV_VARS
        .iter()
        .copied()
        .filter(|k| std::env::var(k).ok().filter(|v| !v.is_empty()).is_some())
        .collect()
}
