//! Configuration and CLI parameters for `openfang-worker`.

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use url::Url;
use zeroize::Zeroizing;

/// Runtime configuration for executing a single job attempt.
#[derive(Clone)]
pub struct WorkerConfig {
    /// Control-plane Server base URL (e.g. `https://api.freeagents.cloud`).
    pub server_url: String,
    /// Short-lived job-scoped credential supplied by Server.
    pub job_token: Zeroizing<String>,
    /// Path to the bounty agent manifest (`agents/freeagents-bounty.toml`).
    pub manifest_path: PathBuf,
    /// Coalescing window for `text_delta` event batches (100–500 ms recommended).
    pub event_flush_interval_ms: u64,
    /// Maximum number of events per HTTP batch submission.
    pub event_max_batch_size: usize,
    /// Interval between attempt lease heartbeats.
    pub heartbeat_interval_ms: u64,
    /// Default model identifier if not specified in the job payload.
    pub default_model: String,
    /// Optional custom web search provider (`duckduckgo` or `searxng`).
    pub web_search_provider: String,
    /// Allow `http://` server URLs (only for local development/tests; HTTPS required in prod).
    pub allow_insecure_http: bool,
}

impl std::fmt::Debug for WorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerConfig")
            .field("server_url", &self.server_url)
            .field("job_token", &"<redacted>")
            .field("manifest_path", &self.manifest_path)
            .field("event_flush_interval_ms", &self.event_flush_interval_ms)
            .field("event_max_batch_size", &self.event_max_batch_size)
            .field("heartbeat_interval_ms", &self.heartbeat_interval_ms)
            .field("default_model", &self.default_model)
            .field("web_search_provider", &self.web_search_provider)
            .field("allow_insecure_http", &self.allow_insecure_http)
            .finish()
    }
}

/// Optional TOML file overrides (`configs/bounty-worker.example.toml`).
#[derive(Debug, Default, Deserialize)]
pub struct WorkerFileConfig {
    pub server_url: Option<String>,
    pub manifest_path: Option<PathBuf>,
    pub event_flush_interval_ms: Option<u64>,
    pub event_max_batch_size: Option<usize>,
    pub heartbeat_interval_ms: Option<u64>,
    pub default_model: Option<String>,
    pub web_search_provider: Option<String>,
}

impl WorkerFileConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read worker config at {}", path.display()))?;
        toml::from_str(&raw)
            .with_context(|| format!("failed to parse worker config at {}", path.display()))
    }
}

impl WorkerConfig {
    /// Validate URL scheme, token presence, and sane batching bounds.
    pub fn validate(&self) -> Result<()> {
        let parsed = Url::parse(&self.server_url)
            .with_context(|| format!("invalid server URL '{}'", self.server_url))?;
        match parsed.scheme() {
            "https" => {}
            "http" if self.allow_insecure_http || is_loopback_host(parsed.host_str()) => {}
            other => bail!(
                "unsupported server URL scheme '{other}': HTTPS is required outside loopback"
            ),
        }

        if self.job_token.as_str().trim().is_empty() {
            bail!("missing job-scoped token (set OPENFANG_JOB_TOKEN or --token-file)");
        }
        if self.event_max_batch_size == 0 {
            bail!("event_max_batch_size must be >= 1");
        }
        Ok(())
    }
}

fn is_loopback_host(host: Option<&str>) -> bool {
    matches!(host, Some("127.0.0.1" | "localhost" | "::1" | "[::1]"))
}

/// Resolve the short-lived job token from `--token-file` or `OPENFANG_JOB_TOKEN`.
pub fn load_job_token(token_file: Option<&Path>) -> Result<Zeroizing<String>> {
    if let Some(path) = token_file {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read job token file {}", path.display()))?;
        let trimmed = content.trim().to_string();
        if trimmed.is_empty() {
            bail!("job token file {} is empty", path.display());
        }
        return Ok(Zeroizing::new(trimmed));
    }

    let env_val = std::env::var("OPENFANG_JOB_TOKEN")
        .context("OPENFANG_JOB_TOKEN is not set and --token-file was not provided")?;
    let trimmed = env_val.trim().to_string();
    if trimmed.is_empty() {
        bail!("OPENFANG_JOB_TOKEN is empty");
    }
    Ok(Zeroizing::new(trimmed))
}
