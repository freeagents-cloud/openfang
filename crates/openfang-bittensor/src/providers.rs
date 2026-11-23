//! Approved Bittensor inference providers (`engy`, `chutes`, `saygm`).
//!
//! No centralized commercial LLM gateways (OpenAI, Anthropic, xAI, Gemini, etc.)
//! are representable in [`BittensorProvider`].

use crate::errors::BittensorError;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// The closed set of Bittensor inference providers permitted in FreeAgents bounty mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BittensorProvider {
    /// Engy — primary live Bittensor inference provider.
    Engy,
    /// Chutes — secondary Bittensor inference provider.
    Chutes,
    /// SayGM — tertiary Bittensor inference provider.
    Saygm,
}

impl BittensorProvider {
    /// Canonical lowercase wire identifier.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Engy => "engy",
            Self::Chutes => "chutes",
            Self::Saygm => "saygm",
        }
    }

    /// All approved Bittensor providers in default priority order.
    pub fn all() -> &'static [BittensorProvider] {
        &[Self::Engy, Self::Chutes, Self::Saygm]
    }

    /// Static descriptor for this provider.
    pub fn descriptor(&self) -> ProviderDescriptor {
        match self {
            Self::Engy => ProviderDescriptor {
                id: Self::Engy,
                display_name: "Engy",
                docs_url: "https://engy.ai/docs",
                supports_inference_proof: true,
            },
            Self::Chutes => ProviderDescriptor {
                id: Self::Chutes,
                display_name: "Chutes",
                docs_url: "https://docs.chutes.ai/",
                supports_inference_proof: false,
            },
            Self::Saygm => ProviderDescriptor {
                id: Self::Saygm,
                display_name: "SayGM",
                docs_url: "https://docs.saygm.com/api/overview/",
                supports_inference_proof: false,
            },
        }
    }
}

impl fmt::Display for BittensorProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for BittensorProvider {
    type Err = BittensorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "engy" => Ok(Self::Engy),
            "chutes" => Ok(Self::Chutes),
            "saygm" => Ok(Self::Saygm),
            other => Err(BittensorError::DisallowedProvider(other.to_string())),
        }
    }
}

/// Static metadata describing an approved Bittensor provider.
#[derive(Debug, Clone, Serialize)]
pub struct ProviderDescriptor {
    pub id: BittensorProvider,
    pub display_name: &'static str,
    pub docs_url: &'static str,
    pub supports_inference_proof: bool,
}
