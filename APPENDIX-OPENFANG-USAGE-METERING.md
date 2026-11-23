# Appendix A — OpenFang: Trustworthy Usage Evidence for Paid Jobs

**Repository:** `freeagents-cloud/openfang`  
**Applies to:** `01-OPENFANG-SPEC.md` (new §12)  
**Status:** Design addendum; billing is a Server responsibility  
**Protocol owner:** Server (`openapi/internal-worker.v1.yaml`)

## A.1 Scope and boundary

The original OpenFang spec's §1 exclusion of **payment functionality** remains in force for the fork. This appendix **does not add Stripe, wallets, financial ledgers, pricing decisions, treasury signing, or billing UI to OpenFang**. It adds the minimal **job-scoped measurement and enforcement hooks** required for the Server to bill accurately using cost-plus-15%.

Unchanged bounty criterion: the **actual Rust reasoning loop** runs on Bittensor CPU and all inference goes through Engy/Chutes/SayGM under the existing fail-closed model policy. No payment path may cause a fallback to centralized inference or non-Bittensor agent CPU.

## A.2 Metering responsibilities

| Signal | Origin / authority | OpenFang responsibility |
|---|---|---|
| Model tokens, model calls, provider/model ID | Inference proxy/provider (Server authoritative for cost) | Attach `model_call_id`, job/attempt, and event timestamps; optionally forward provider usage frames for diagnostics |
| Provider unit cost / cost in USD micros | Server after provider reconciliation | **Never** calculate customer price or assume exact provider rates |
| Tool invocations | OpenFang agent loop | Emit start/end, status, sandbox/runtime ID, capped counts; mark separately billable paid tool usage |
| CPU sandbox runtime/cost | Server/compute provider (Cathedral/ms.computer) | Emit worker lifecycle and heartbeats; do not self-report its own billable provider price |
| Errors, cancellation, retries | OpenFang + Server job attempt state | Preserve unique attempt identity and deterministic status/evidence |
| Customer charge (1.15 × eligible provider cost) | Server pricing + ledger | No price calculation, wallet transfers, account queries or credit writes |

Explicitly model **observed usage** separately from **provider invoiced charge**. Tool/token counts are evidence, not receipts. Charges may be available only after the provider's accounting system settles them.

## A.3 Worker job contract extensions

Preserve existing `{job_id, attempt, protocol_version:1, trace_id}`. Add backward-compatible fields in a reviewed revision of `internal-worker.v1.yaml` (increment schema minor version; reject unknown required protocol versions):

```json
{
  "billing_context": {
    "cost_policy_id": "cost-plus-15-v1",
    "max_provider_cost_usd_micro": 2000000,
    "max_model_calls": 10,
    "max_tool_calls": 6,
    "metering_required": true
  }
}
```

This payload is **policy only**, not a payment token or user account balance. The worker may **reduce** its own limits but must never expand a limit supplied by Server. No balances, wallet addresses, customer card details, Stripe IDs or treasury addresses are required inside a sandbox. Any existing `limits` object remains the source for max tokens/deadline; billing context refines, never weakens, it.

Add structured events **in addition to** existing `started`, `usage`, `checkpoint` and terminal events. New event types are internal, redacted and authenticated:

- `model_call_started`: `{model_call_id, provider_requested, model_requested, started_at}`.
- `model_call_finished`: `{model_call_id, provider_actual, model_actual, provider_request_id?, input_tokens?, output_tokens?, usage_source, finish_reason, completed_at}`. **Proxy's own usage/cost records remain authoritative**.
- `tool_metered`: `{tool_call_id, tool_kind, started_at, ended_at, success, provider_charge_ref?}`; no secrets, raw headers or prompt payloads.
- `budget_warning` / `budget_exhausted`: `{limit_kind, observed_value, cap, safe_abort}` with no credit balance.
- Terminal `completed` / `failed` / `cancelled`: include `model_call_ids`, `tool_call_ids`, `attempt`, `finish_reason`, `request_ids_seen` and durations. **Do not embed a total customer price**.

All events must preserve original ordering and idempotent `(job_id, attempt, sequence)` semantics. `model_call_id` and `tool_call_id` must be **stable across HTTP event retries** and unique per logical call, even if transport retries occur. Mark ambiguous transport retries for Server reconciliation instead of pretending a provider generated one charge.

## A.4 Runtime behavior and enforcement

1. Worker fetches one authorized job and `billing_context`. Reject missing required policy, mismatched job/attempt or expired scoped credentials before invoking any provider.
2. Start agent loop with limits on wall clock, model turns, tool count, total tokens, and output size. Existing **Server inference proxy** enforces the provider-cost cap and is authoritative for model billing.
3. For each model call generate stable `model_call_id`; pass it and job/attempt through a scoped `X-Worker-Model-Call-ID` header to the Server inference proxy (subject to canonical protocol validation). Do not send arbitrary customer-provided headers to providers.
4. Parse token usage **when supported**, including final streamed usage frames; emit honest `unknown` or `estimate` when final usage is absent. Never synthesize a claimed billable rate.
5. On `budget_exhausted` from Server, terminate the current loop gracefully, flush ordered usage/events and signal bounded partial/failure outcome. The worker cannot issue another model call under a new ID to bypass cap.
6. For all tools, emit `tool_metered` with stable IDs and safe metadata. Cost-bearing tools must be explicitly allowlisted and route through Server-approved proxies; no in-sandbox purchase capability.
7. On crash/cancellation, attempt to flush final events; Server independently reconciles provider-side charges even when OpenFang never emits `completed`.
8. On requeued attempts, preserve the **new attempt number**. Do not resend earlier attempts' usage as if it were new; Server aggregates eligible unique provider charges across attempts and applies customer retry policy.
9. Disable unbounded upstream background loops, skill installation and other spontaneous side-effects in paid worker mode. No job may execute after its lease/token expires.

An **OpenFang agent running inside a paid Cathedral sandbox is not authorized to execute billing code**, even if it can make outbound HTTP requests. Server ensures scoped routes expose only job progress, bounded inference and job completion, not customer payment operations.

## A.5 Proposed incremental file layout

```text
openfang/
├── crates/
│   ├── openfang-bittensor/src/
│   │   ├── metering.rs             # model call IDs and usage envelopes
│   │   └── proxy_client.rs         # attach scoped call identifiers; parse cap errors
│   ├── openfang-worker/src/
│   │   ├── billing_policy.rs       # consume max-cost policy; NOT charge customers
│   │   ├── events.rs               # structured usage events / dedupe
│   │   ├── runner.rs               # per-job budget-aware execution
│   │   └── cancellation.rs         # spend-cap exhaustion -> graceful stop
│   └── openfang-runtime/          # smallest necessary hook for call/tool IDs
├── tests/bounty/
│   ├── metering_streaming.rs
│   ├── provider_usage_missing.rs
│   ├── retry_deduplication.rs
│   ├── budget_exhaustion.rs
│   └── no_wallet_or_billing_secrets.rs
└── docs/METERING-PROTOCOL.md
```

Path suggestions augment the existing fork layout and should be adapted after pinning/analyzing the actual upstream commit; do not unnecessarily fork upstream LLM provider logic to implement metering.

## A.6 Tests and definition of done

- **Correct identity:** all model/tool events contain assigned `job_id`, `attempt`, stable call IDs and monotonic sequence; duplicate event submissions don't create duplicate chargeable usage.
- **Streaming:** when a Bittensor model returns a final usage frame, record it once; when not returned, leave usage status `unknown` until Server resolves it.
- **Fail closed:** worker cannot select a non-Bittensor provider, call inference after a hard cap, mutate priced job policy, or invoke any billing/treasury operation.
- **Retry:** one logical provider request with an event retry is not counted twice; ambiguous upstream replays remain separately identifiable for reconciliation.
- **Crash:** Server can detect provider-side costs and enforce the hold even if the worker is killed before completion.
- **Security:** no Stripe secret, TAO private key, user wallet data, provider long-lived key or database URL is present in worker image/env/job/events.
- **Bounty compatibility:** usage metering remains enabled for public paid tasks; private integration tests can use synthetic credentials and mock provider cost events without making real payment transactions.

**Integration contract:** Server owns all customer money, provider charges and accounting; OpenFang owns only its agent loop and safe, stable usage/provenance identifiers.

## A.7 Chutes default-model and model switching contract (day one)

- The Worker does **not** decide which model is free or what a customer pays. Server selects the model from its allowlisted catalog and job snapshot. On new conversations the default snapshot is: `provider=chutes`, `model_id=deepseek-ai/DeepSeek-V4.1-Flash-1017`.
- Each leased job includes `selected_model: { provider, model_id, pricing_version, capabilities }` (nonsecret) in the Server-owned versioned protocol. The worker validates the actual model against its permitted Bittensor provider policy and passes the exact ID to `POST /internal/v1/inference/chat`. Server verifies `provider/model` authorization again.
- Do not fall back to the former `engy` default hardcoded in the original fork proposal. Any fallback is allowed only through Server's explicit job policy, recorded on the response, and budget checked.
- All inference calls, including history compaction or self-reflection, count against the authorized job budget. Inference request usage must identify actual provider/model even when it differs from the selected model due to an approved fallback.
- No inference provider is assumed to be free. Open-weight model licensing is unrelated to hosted Chutes cost, and Bittensor CPU remains chargeable independently. The worker never routes traffic through unverified free-tier credentials or multiple accounts to evade provider limits.

**Example job field (schematic; extend Server internal-worker OpenAPI):**

```json
{
  "selected_model": {
    "provider": "chutes",
    "model_id": "deepseek-ai/DeepSeek-V4.1-Flash-1017",
    "pricing_version": "chutes-catalog-observed-2026-11-22",
    "capabilities_verified": false
  }
}
```

`capabilities_verified` remains `false` until an actual tool-call and streaming compatibility probe succeeds. The Server model catalog is authoritative for the supported capabilities. Reject tool-needing jobs until compatibility is confirmed.

**Tests**: default job executes through Chutes; changed model uses exact selected ID; unauthorized model cannot be smuggled into an internal proxy request; provider usage is billed once across retries; a purported `free=true` client flag has zero effect on cost enforcement.
