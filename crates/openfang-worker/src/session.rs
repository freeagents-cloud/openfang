//! Ephemeral per-job session state and isolated bounty agent manifest loading.
//!
//! No shared SQLite database or global memory namespace is ever mounted in bounty mode.

use crate::protocol::JobAssignment;
use anyhow::{Context, Result};
use openfang_memory::session::Session;
use openfang_memory::MemorySubstrate;
use openfang_types::agent::{AgentId, AgentManifest, AutonomousConfig, SessionId};
use openfang_types::message::Message;
use std::path::Path;
use tempfile::TempDir;
use uuid::Uuid;

/// Isolated ephemeral workspace + in-memory substrate for a single job attempt.
///
/// Automatically removes the temporary directory when dropped.
pub struct EphemeralJobState {
    pub temp_dir: TempDir,
    pub memory: MemorySubstrate,
    pub session: Session,
    pub latest_user_message: String,
    pub manifest: AgentManifest,
}

impl EphemeralJobState {
    /// Create fresh, isolated per-job state from the Server's `JobAssignment`
    /// and the bounty manifest file.
    pub fn build(job: &JobAssignment, manifest_path: &Path, default_model: &str) -> Result<Self> {
        let temp_dir = tempfile::Builder::new()
            .prefix(&format!("openfang-job-{}-", job.job_id))
            .tempdir()
            .context("failed to create ephemeral per-job temp directory")?;

        // Pure in-memory SQLite substrate — zero cross-job persistence or shared memory.
        let memory = MemorySubstrate::open_in_memory(0.05)
            .map_err(|e| anyhow::anyhow!("failed to initialize ephemeral MemorySubstrate: {e}"))?;

        let mut manifest = load_bounty_manifest(manifest_path)?;

        // Defensively enforce bounty-mode constraints on the loaded manifest:
        // 1. Clear any upstream fallback_models (no centralized provider fallbacks).
        manifest.fallback_models.clear();
        // 2. Disable memory/shell/agent-messaging capabilities.
        manifest.capabilities.memory_read.clear();
        manifest.capabilities.memory_write.clear();
        manifest.capabilities.agent_message.clear();
        manifest.capabilities.shell.clear();
        // 3. Restrict tools to intersection of Server allowlist and bounty read-only tools.
        manifest.capabilities.tools = job
            .allowed_tools
            .iter()
            .filter(|t| matches!(t.as_str(), "web_search" | "web_fetch"))
            .cloned()
            .collect();
        // 4. Bind authoritative model ID (selected_model > job.model > default) and hard limits.
        let model_id = job
            .selected_model
            .as_ref()
            .map(|s| s.model_id.as_str())
            .or(job.model.as_deref())
            .filter(|m| !m.trim().is_empty())
            .unwrap_or(default_model)
            .to_string();
        manifest.model.model = model_id;
        manifest.model.max_tokens = (job.limits.max_tokens.min(u64::from(u32::MAX)) as u32)
            .min(manifest.model.max_tokens.max(1024));
        manifest.autonomous = Some(AutonomousConfig {
            max_iterations: job.limits.max_agent_turns,
            ..Default::default()
        });

        // Separate prior conversation history from the latest user turn.
        let mut history: Vec<Message> = Vec::with_capacity(job.messages.len());
        for m in &job.messages {
            match m.role.to_ascii_lowercase().as_str() {
                "assistant" => history.push(Message::assistant(&m.content)),
                "user" => history.push(Message::user(&m.content)),
                // System messages are controlled by the bounty manifest, not untrusted turns.
                _ => history.push(Message::user(&m.content)),
            }
        }

        let latest_user_message = match history.pop() {
            Some(last) => last.content.text_content(),
            None => String::new(),
        };

        let session = Session {
            id: SessionId(Uuid::new_v4()),
            agent_id: AgentId(Uuid::new_v4()),
            messages: history,
            context_window_tokens: 0,
            label: Some(format!("job:{}:attempt:{}", job.job_id, job.attempt)),
        };

        Ok(Self {
            temp_dir,
            memory,
            session,
            latest_user_message,
            manifest,
        })
    }
}

/// Load `freeagents-bounty.toml` or fall back to an embedded minimal manifest if absent.
fn load_bounty_manifest(path: &Path) -> Result<AgentManifest> {
    if path.exists() {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read agent manifest at {}", path.display()))?;
        let manifest: AgentManifest = toml::from_str(&raw)
            .with_context(|| format!("failed to parse agent manifest at {}", path.display()))?;
        Ok(manifest)
    } else {
        let embedded = include_str!("../../../agents/freeagents-bounty.toml");
        let manifest: AgentManifest =
            toml::from_str(embedded).context("failed to parse embedded freeagents-bounty.toml")?;
        Ok(manifest)
    }
}
