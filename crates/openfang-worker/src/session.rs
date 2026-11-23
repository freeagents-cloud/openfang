//! Ephemeral per-job session state and isolated bounty agent manifest loading.
//!
//! No shared SQLite database or global memory namespace is ever mounted in bounty mode.

use crate::protocol::JobAssignment;
use anyhow::{Context, Result};
use openfang_memory::session::Session;
use openfang_memory::MemorySubstrate;
use openfang_types::agent::{AgentId, AgentManifest, AutonomousConfig, SessionId};
use openfang_types::config::MemoryConfig;
use openfang_types::message::Message;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use uuid::Uuid;

/// Deterministic agent ID within a tenant's isolated SQLite `MemorySubstrate`
/// so cross-conversation RAG (`remember`/`recall`) links facts across conversations
/// for the same user while never crossing tenant boundaries.
pub const TENANT_AGENT_ID: AgentId = AgentId(Uuid::nil());

/// Isolated per-job workspace + tenant-scoped SQLite `MemorySubstrate`.
///
/// Automatically removes the temporary directory when dropped.
pub struct EphemeralJobState {
    pub temp_dir: TempDir,
    pub memory_db_path: PathBuf,
    pub memory: MemorySubstrate,
    pub session: Session,
    pub latest_user_message: String,
    pub manifest: AgentManifest,
}

impl EphemeralJobState {
    /// Create fresh, isolated per-job state from the Server's `JobAssignment`
    /// and the bounty manifest file (without pre-populated memory).
    pub fn build(job: &JobAssignment, manifest_path: &Path, default_model: &str) -> Result<Self> {
        Self::build_with_memory(job, manifest_path, default_model, None)
    }

    /// Create per-job state backed by OpenFang's real SQLite `MemorySubstrate`
    /// hydrated from the tenant's persisted SQLite blob (if any).
    pub fn build_with_memory(
        job: &JobAssignment,
        manifest_path: &Path,
        default_model: &str,
        persisted_memory_sqlite: Option<&[u8]>,
    ) -> Result<Self> {
        let temp_dir = tempfile::Builder::new()
            .prefix(&format!("openfang-job-{}-", job.job_id))
            .tempdir()
            .context("failed to create ephemeral per-job temp directory")?;

        let memory_db_path = temp_dir.path().join("memory.db");
        if let Some(bytes) = persisted_memory_sqlite {
            if !bytes.is_empty() {
                std::fs::write(&memory_db_path, bytes)
                    .context("failed to hydrate tenant memory.db")?;
            }
        }

        // OpenFang's real SQLite-backed MemorySubstrate (semantic RAG + structured + knowledge).
        let memory = MemorySubstrate::open(&memory_db_path, 0.05, &MemoryConfig::default())
            .map_err(|e| anyhow::anyhow!("failed to initialize tenant MemorySubstrate: {e}"))?;

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
            agent_id: TENANT_AGENT_ID,
            messages: history,
            context_window_tokens: 0,
            label: Some(format!("job:{}:attempt:{}", job.job_id, job.attempt)),
        };

        Ok(Self {
            temp_dir,
            memory_db_path,
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

#[cfg(test)]
mod tests {
    use super::*;
    use openfang_types::memory::{Memory, MemoryFilter, MemorySource};
    use std::collections::HashMap;

    #[tokio::test]
    async fn cross_conversation_rag_persists_and_recalls_across_sqlite_hydration() {
        let dir1 = tempfile::tempdir().unwrap();
        let db1 = dir1.path().join("memory.db");
        {
            let mem1 = MemorySubstrate::open(&db1, 0.05, &MemoryConfig::default()).unwrap();
            mem1.remember(
                TENANT_AGENT_ID,
                "User prefers dark roast espresso and uses Rust on macOS.",
                MemorySource::Conversation,
                "conversation",
                HashMap::new(),
            )
            .await
            .unwrap();
        }
        let persisted_bytes = std::fs::read(&db1).unwrap();
        assert!(!persisted_bytes.is_empty());

        // Hydrate into a brand-new sandbox directory (Conversation 2) and recall.
        let dir2 = tempfile::tempdir().unwrap();
        let db2 = dir2.path().join("memory.db");
        std::fs::write(&db2, &persisted_bytes).unwrap();
        let mem2 = MemorySubstrate::open(&db2, 0.05, &MemoryConfig::default()).unwrap();
        let recalled = mem2
            .recall("espresso", 5, Some(MemoryFilter::agent(TENANT_AGENT_ID)))
            .await
            .unwrap();
        assert!(
            recalled.iter().any(|f| f.content.contains("dark roast espresso")),
            "expected cross-conversation RAG recall to find persisted fact, got: {recalled:?}"
        );
    }
}
