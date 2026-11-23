# FreeAgents Bounty — OpenFang Fork Baseline Audit

**Repository:** `freeagents-cloud/openfang`  
**Upstream:** `https://github.com/RightNow-AI/openfang`  
**Specification:** [01-OPENFANG-SPEC.md](../01-OPENFANG-SPEC.md)  
**Audit Date:** 2026-11-22

---

## 1. Fork Provenance & Licenses

| Field | Value |
|---|---|
| `UPSTREAM_SHA` | `acf2587e46be174c10200489c9a2d23a39a98aeb` (`v0.6.9`) |
| Fork Baseline `HEAD` | `7759a920abb7e4bd47183a368ac27a7c9861b204` |
| Upstream License | `Apache-2.0 OR MIT` ([LICENSE-APACHE](../LICENSE-APACHE), [LICENSE-MIT](../LICENSE-MIT)) |
| Rust Toolchain | `stable` (`rustc 1.97.1`, `cargo 1.97.1`, MSRV `1.75`, edition `2021`) |
| Baseline Check | `cargo check -p openfang-types -p openfang-memory -p openfang-runtime` — **PASS** |

All upstream notices, attribution, and dual licensing (`Apache-2.0 OR MIT`) are preserved intact.

---

## 2. Upstream Workspace Crate Inventory

Captured via `cargo metadata --no-deps --format-version 1` prior to adding FreeAgents crates:

| Crate | Version | License | Role in Bounty Mode |
|---|---|---|---|
| `openfang-types` | 0.6.9 | Apache-2.0 OR MIT | Reused — message, tool, agent manifest, config, error types |
| `openfang-memory` | 0.6.9 | Apache-2.0 OR MIT | Reused — ephemeral per-job `MemorySubstrate::open_in_memory` |
| `openfang-runtime` | 0.6.9 | Apache-2.0 OR MIT | Reused — `run_agent_loop_streaming`, `LlmDriver`, `OpenAIDriver`, `WebToolsContext`, `LoopGuard`, `compactor` |
| `openfang-skills` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; **disabled** in bounty worker mode |
| `openfang-wire` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; **disabled** in bounty worker mode |
| `openfang-api` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; **not** the public FreeAgents API |
| `openfang-channels` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; **disabled** in bounty worker mode |
| `openfang-extensions` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; **disabled** in bounty worker mode |
| `openfang-hands` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; **disabled** in bounty worker mode |
| `openfang-kernel` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; bypassed by `openfang-worker` |
| `openfang-migrate` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; unused in bounty mode |
| `openfang-cli` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; unused in bounty mode |
| `openfang-desktop` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace; unused in bounty mode |
| `xtask` | 0.6.9 | Apache-2.0 OR MIT | Retained in workspace |

### Added FreeAgents Workspace Crates
- `openfang-bittensor`: Bittensor-only (`engy`, `chutes`, `saygm`) model policy, Server inference proxy driver (`BittensorProxyDriver` wrapping `OpenAIDriver`), and per-call telemetry.
- `openfang-worker`: Stateless, outbound-only job worker binary (`openfang-worker run` / `serve`) speaking `internal-worker.v1` and executing `openfang_runtime::agent_loop::run_agent_loop_streaming`.

---

## 3. Verified Upstream Entry Points

| Capability | Location | Notes |
|---|---|---|
| Core streaming agent loop | `crates/openfang-runtime/src/agent_loop.rs` (`run_agent_loop_streaming`) | Accepts `manifest`, `user_message`, `&mut Session`, `&MemorySubstrate`, `Arc<dyn LlmDriver>`, `&[ToolDefinition]`, `mpsc::Sender<StreamEvent>`, `Option<&WebToolsContext>`, `Option<&PhaseCallback>`, and optional handles (`None` in bounty mode). |
| LLM driver abstraction | `crates/openfang-runtime/src/llm_driver.rs` (`LlmDriver`, `CompletionRequest`, `StreamEvent`) | `complete()` and `stream()` async trait methods. |
| OpenAI-compatible driver | `crates/openfang-runtime/src/drivers/openai.rs` (`OpenAIDriver`) | Handles SSE streaming, tool-call delta assembly, think-tag filtering, and usage parsing. Extended minimally with `with_chat_endpoint(...)` to hit Server's `/internal/v1/inference/chat`. |
| Guarded web tools | `crates/openfang-runtime/src/web_search.rs`, `web_fetch.rs`, `tool_runner.rs` (`builtin_tool_definitions`) | `WebFetchEngine` enforces SSRF checks (`check_ssrf`), HTML→Markdown extraction, and `wrap_external_content()`. |
| Ephemeral session/memory | `crates/openfang-memory/src/substrate.rs` (`MemorySubstrate::open_in_memory`) | Pure in-memory SQLite per job; zero shared state across jobs. |
| Loop guard & compaction | `crates/openfang-runtime/src/loop_guard.rs`, `compactor.rs`, `context_budget.rs` | Preserved directly inside `run_agent_loop_streaming`. |
