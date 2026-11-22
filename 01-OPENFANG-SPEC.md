# FreeAgents — OpenFang Fork Implementation Specification

**Repository:** `freeagents-cloud/openfang`  
**Document:** `01-OPENFANG-SPEC.md`  
**Status:** Proposed engineering specification  
**Date:** 2026-11-22  
**Owner:** Agent runtime team  
**Depends on:** Server internal worker protocol v1

## 1. Purpose and bounty acceptance

Develop a real, publicly auditable **fork of OpenFang** that acts as a GrokBot-style conversational agent whose **actual Rust agent loop executes on a Bittensor CPU machine** and whose **LLM inference is supplied by Bittensor services**. The minimum working chain is:

`Web -> Server -> Cathedral CPU sandbox -> OpenFang-derived Rust worker -> Server inference proxy -> Engy -> Worker -> Server -> Web`.

An answer computed on an ordinary cloud VM with only its inference routed through Engy does **not** meet our internal acceptance test. All production inference used by the bounty mode must be from Engy, Chutes, or SayGM; there must be no silent failover to OpenAI, xAI, Anthropic, or other centralized commercial model gateways. Public web-search tools are allowed, but the **model execution** must follow the Bittensor-only policy.

### In scope

- Preserve the GitHub fork ancestry of OpenFang, upstream notices, attribution, and applicable licenses.
- Reuse the Rust agent loop, tool-call protocol, tool execution safeguards, session compaction, and streaming machinery as far as possible.
- Add Bittensor model-provider routing and a remote-worker binary.
- Make the agent callable as a **bounded, stateless job** with external conversation history and streaming events.
- Containerize the worker as a reproducible, pinned OCI image for Cathedral; target ms.computer after its deployment interface is validated.
- Produce execution metadata and receipts for the bounty demonstration.

### Out of scope

Workflows, Hands, cron, skill marketplace, arbitrary plugin installation, autonomous social networking, payment functionality, agent-to-agent collaboration, self-modifying skills, and a user-owned persistent agent VM. These upstream features can remain in the fork but **must not be enabled in bounty worker mode**.

## 2. Upstream baseline and inspection task

OpenFang's documented workspace contains `openfang-types`, `openfang-memory`, `openfang-runtime`, `openfang-kernel`, `openfang-api`, and other crates. Its agent loop is in `openfang-runtime`, with an `LlmDriver` abstraction and an OpenAI-compatible driver; its normal persistence is SQLite. See upstream [architecture](https://github.com/RightNow-AI/openfang/blob/main/docs/architecture.md) and [contributing guide](https://github.com/RightNow-AI/openfang/blob/main/CONTRIBUTING.md).

**First PR must document:** `UPSTREAM_SHA`, fork HEAD SHA, `cargo metadata` crate inventory, build/test results, licenses, and the actual locations of agent execution entry points. The directory suggestions below are an implementation plan, **not a claim that those added files already exist**. Do not mechanically replace upstream's crate graph or delete unrelated upstream crates just to produce a smaller fork.

## 3. Proposed repository file layout

```text
openfang/                         # Existing upstream fork; retain its structure
├── Cargo.toml                    # Add new workspace members only
├── Cargo.lock
├── crates/
│   ├── openfang-types/           # Existing; add only minimal run/usage types if needed
│   ├── openfang-runtime/         # Existing; inject provider and job event hooks
│   ├── openfang-memory/          # Existing; add ephemeral job adapter if needed
│   ├── openfang-kernel/          # Existing; add minimal job-oriented boot path
│   ├── openfang-api/             # Existing; NOT the public FreeAgents API
│   ├── ...                       # Retain remaining upstream crates
│   ├── openfang-bittensor/       # NEW: provider configuration and policy
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── providers.rs       # engy / chutes / saygm descriptors
│   │       ├── models.rs          # Discover/allowlist available models
│   │       ├── proxy_client.rs    # Call Server inference proxy over HTTPS
│   │       ├── policy.rs          # Bittensor-only enforcement
│   │       ├── errors.rs
│   │       └── telemetry.rs
│   └── openfang-worker/          # NEW: executable running on Bittensor CPU
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs           # run-once and poll-loop modes
│           ├── config.rs
│           ├── protocol.rs       # internal-worker.v1 schema client
│           ├── fetch_job.rs
│           ├── runner.rs         # launch OpenFang agent loop
│           ├── session.rs        # per-job isolated session state
│           ├── events.rs         # stream/batch events and checkpoints
│           ├── heartbeat.rs
│           ├── cancellation.rs
│           └── shutdown.rs
├── agents/
│   └── freeagents-bounty.toml    # NEW: minimal read-only Grok-style agent manifest
├── docker/
│   ├── Dockerfile.worker         # NEW: pinned, non-root OCI worker build
│   └── worker-entrypoint.sh      # Only if essential; prefer Rust entry point
├── configs/
│   └── bounty-worker.example.toml
├── tests/
│   └── bounty/
│       ├── openai_compat.rs
│       ├── provider_fail_closed.rs
│       ├── agent_tool_cycle.rs
│       ├── tenant_isolation.rs
│       ├── job_protocol.rs
│       └── cancellation.rs
├── docs/
│   ├── FREEAGENTS-BOUNTY.md
│   ├── WORKER-PROTOCOL.md
│   └── EVIDENCE.md
└── .github/workflows/
    └── bounty-worker.yml
```

`openfang-bittensor` must not implement a second chat-completions stack unnecessarily. Reuse the upstream OpenAI-compatible driver where it can preserve streaming and tool-use semantics; add only configuration, routing restrictions, required headers, and proxy plumbing.

## 4. Rust worker contract

### 4.1 Worker invocation

Two modes sharing **exactly the same job executor**:

```bash
openfang-worker run --job-id <uuid> --server https://api.freeagents.cloud
openfang-worker serve --server https://api.freeagents.cloud
```

- `run`: Cathedral execution, one specific job, then exit with success/failure status. This is the **first required mode**.
- `serve`: optional long-running ms.computer worker that leases jobs via an outbound connection. Implement only after the on-demand mode is proven.
- No inbound TCP port is required inside Cathedral; all calls originate outbound to Server.
- Worker is started with **one short-lived job-scoped credential** (or a short-lived sandbox identity for serve mode), never a PostgreSQL URL, Cathedral API key, or provider's permanent API key.

### 4.2 Job request v1

The canonical schema is owned by `server/openapi/internal-worker.v1.yaml`; the fork vendors a pinned generated or manually validated Rust representation. The logical payload is:

```json
{
  "protocol_version": 1,
  "job_id": "uuid",
  "attempt": 1,
  "conversation_id": "uuid",
  "messages": [{"role": "user", "content": "Explain this subnet"}],
  "system_policy_version": "bounty-v1",
  "model_policy": {"allowed_providers": ["engy", "chutes", "saygm"], "preferred": "engy"},
  "allowed_tools": ["web_search", "web_fetch"],
  "limits": {"max_agent_turns": 8, "max_tokens": 12000, "deadline_ms": 90000},
  "trace_id": "trace-opaque"
}
```

Values above are **illustrative defaults**, not provider-validated limits. `job_id` and `attempt` form an immutable execution identity. `conversation_id` is an opaque reference; the worker must not enumerate other conversations or tenants. The Server chooses the provider policy, tool allowlist, deadlines, and budgets. The worker cannot increase them.

### 4.3 Events and completion

Events have `{job_id, attempt, sequence, type, at, data}`. Supported types:

`started`, `provider_selected`, `text_delta`, `tool_started`, `tool_finished`, `source_found`, `usage`, `checkpoint`, `completed`, `failed`, `cancelled`.

Worker sends ordered event batches to Server; it retries safely with `(job_id, attempt, sequence)` idempotency. A failed HTTP call must not make it drop completion or emit duplicates. Terminal events include `message`, `usage`, `provider`, `model`, `finish_reason`, and timings. The Server is authoritative for final job status and delivery to the UI.

For MVP text streaming, emit coalesced chunks (e.g., 100–500 ms) rather than one HTTP request per token. Tool inputs/results containing credentials or personal data must be redacted from public events.

## 5. Agent-loop integration

### 5.1 Minimal chat agent

- Create one reusable agent configuration (`freeagents-bounty.toml`) with a clear, factual, non-deceptive identity.
- Accept history supplied by Server rather than depending on a global persistent agent profile.
- Disable background loops, Hands, workflow triggers, inter-agent delegation, package installation, and autonomous outbound messaging.
- Allow web search/fetch only behind guarded tools. Prompt-injected pages are untrusted data.
- Return citations as structured `source_found` records (URL, title, retrieval timestamp and source ID); do **not** fabricate URLs or treat LLM-provided citations as verified.
- Preserve OpenFang's loop guard, context compaction, tool-call parsing, timeouts and capability gates.

### 5.2 Session storage

No shared SQLite database on the remote CPU worker. Use **per-job ephemeral state** (in-memory or a temporary SQLite file under a job-specific directory) solely to satisfy upstream components that require SQLite. Server is the source of truth for conversations. Delete the temporary state at job termination and sandbox disposal. Explicitly disable any upstream **global/shared memory namespace** for bounty mode.

### 5.3 Tool isolation

The Rust agent loop and its allowed tools run **inside the Bittensor CPU sandbox**. Limit built-in tools to `web_search` and `web_fetch` for the first demonstration. If shell or Python execution is later enabled, execute in a separate nested sandbox or separate isolated task; do not expose the worker container's control-plane credentials to arbitrary shell commands. Disable network access except preapproved destinations where supported by the compute backend.

## 6. Bittensor inference implementation

### 6.1 Provider selection

Implement a `BittensorModelPolicy` checked at startup **and before every model request**. Allowed providers are Engy, Chutes, SayGM only; provider IDs must be explicitly mapped to their endpoint/model IDs. `Auto` is permitted only among these providers.

- **Engy**: first live integration. Documentation: https://engy.ai/docs
- **Chutes**: second integration. Documentation: https://docs.chutes.ai/
- **SayGM**: third integration. Documentation: https://docs.saygm.com/api/overview/

Because provider model catalogs and support for streaming/tool-call frames may change, run capability probes and record supported model IDs; do not hardcode an unsupported model or promise all tools work on all models.

### 6.2 Inference routing and security

The worker calls Server's scoped inference proxy (`POST /internal/v1/inference/chat`) using its short-lived job token. The Server forwards to an allowed Bittensor provider and keeps the real provider API key in its own secret manager. A job-level budget and model allowlist are enforced **before** forwarding. Proxy results (including streaming) must preserve tool-call identifiers and token accounting.

The HTTP proxy route is a **transport boundary**, not a second LLM reasoning loop: the OpenFang worker decides when to call the model and when to invoke tools. All assistant reasoning/model calls—including summarization/compaction, if enabled—must use the same Bittensor-only route.

### 6.3 Provider provenance

For each model call, record `provider`, `provider_request_id` if present, `model_id`, `started_at`, `finished_at`, `input_tokens`, `output_tokens`, `error`, and `trace_id`. Engy's optional inference proof can be stored and verified independently where supported; a generic API response is not proof of model weights or decentralization.

## 7. Packaging and execution on Bittensor CPU

### Required target: Cathedral

- Build a pinned Linux `x86_64` OCI image containing the Rust worker; record its immutable digest.
- Use Cathedral's **current confidential sandbox** endpoints, not legacy closed Standard sandbox routes. Follow https://cathedral.computer/docs/api/sealed-sandboxes/ and https://cathedral.computer/docs/sandboxes/custom-images/.
- Server creates sandbox using catalog-supported size, `required_hardware: "amd_sev_snp"`, `max_spend_usd`, maximum lifetime, idempotency key, and controlled egress allowlist to `api.freeagents.cloud` (and only any additional endpoints strictly required).
- Use sync `exec` for jobs shorter than Cathedral's documented limit; use **background exec** plus polling for longer jobs. Validate current request encryption behavior: background exec is documented as plaintext to Cathedral's API; do not send long-lived secrets or sensitive user data in commands. Fetch job data from Server over TLS using a job-scoped token inside the sandbox instead.
- Collect Cathedral receipts and associate them with the image digest, sandbox ID, job ID, and timestamps. Receipts prove platform-described execution/attestation properties, **not** that the answer is correct.
- Ensure all commands and data are supported by the selected image/template. Dispose of sandbox after use and on failures.

### Optional target: ms.computer

Add a deploy adapter and warm worker mode after the ms.computer application/container management API, ingress/egress, persistence, credential handling, and capacity limits are tested with the current vendor service. **Do not invent or depend on undocumented endpoints.** The same OCI worker and Server protocol should run there without modifying the agent loop. Reference: https://ms.computer/.

## 8. Failure behavior and security

- Reject any job whose `model_policy` includes a non-Bittensor provider.
- Bound wall clock, total tokens, model calls, tool calls, output bytes, and sandbox spend.
- If Engy fails, optionally retry with Chutes/SayGM **only before text is exposed or via a clearly logged replacement attempt**; never splice unrelated model generations into one answer without attribution.
- On timeout/cancel, stop the agent loop, flush terminal status if possible, and exit. Server owns retry/fencing decisions.
- No durable user credential, payment key, or database password in sandbox environment.
- Log trace IDs, attempts, provider names, model IDs, and safe diagnostics; redact conversation body by default.
- Treat all source text, web pages, and model outputs as untrusted.

## 9. Required tests and done definition

| Test | Pass condition |
|---|---|
| Build and upstream compatibility | `cargo check`, `cargo test`, `cargo clippy` on pinned fork, with documented upstream baseline |
| Provider routing | Engy streaming, tool-calling, and usage parsing work; unsupported feature rejected clearly |
| Bittensor-only | All nonallowed provider selection/fallback attempts fail closed |
| Full loop | User request -> one or more Bittensor model calls -> tool -> grounded final answer |
| Session isolation | Two concurrent jobs cannot observe each other's memory or files |
| Remote execution | Actual worker binary runs inside Cathedral sandbox, not only a control-plane proxy |
| Evidence | Job points to Cathedral sandbox and available signed receipts plus Bittensor model call metadata |
| Retry and cancel | Job token revocation, duplicate batch submission, lease expiration, and cancellation behave safely |
| Reproducibility | Pinned image digest, build commit SHA, and README demo instructions published |

## 10. Implementation order

1. Fork-baseline audit and pinned commit.
2. Build `openfang-worker run` locally with an injected minimal OpenFang chat loop.
3. Add Server job fetch/event submission protocol with fake inference tests.
4. Add Engy-backed inference proxy client and Bittensor-only guard.
5. Run compiled worker **inside Cathedral** and save one successful complete trace and receipt.
6. Add web search and source events.
7. Chutes and SayGM, fallback tests, operational hardening.
8. `serve` mode / ms.computer only if provider support and bounty deadline allow.

## 11. Open questions / implementation gates

- Audit the **actual commit** in `freeagents-cloud/openfang` visible in the organization's screenshot. This spec has not inspected that fork's source tree, and upstream branches have changed over time.
- Verify Cathedral image import/boot semantics and account concurrency/credits in a real account.
- Verify each chosen model's tool-call and SSE behavior before making it the default.
- Determine whether Jacob requires the public X account itself. An X automated AI reply bot requires prior explicit platform approval under the current X automation rules; keep this as a separate deployment gate.

**Authoritative cross-repo contract:** `freeagents-cloud/server/openapi/internal-worker.v1.yaml`.
