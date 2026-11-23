# FreeAgents OpenFang Metering Protocol (Appendix A)

**Canonical contract:** `freeagents-cloud/server/openapi/internal-worker.v1.yaml` (`v1.1.0`)  
**Specification:** [APPENDIX-OPENFANG-USAGE-METERING.md](../APPENDIX-OPENFANG-USAGE-METERING.md)

## 1. Separation of Responsibilities

- **Server is authoritative** for all customer billing, provider invoiced charges, credit holds, and cost-plus-15% settlement.
- **`openfang-worker` is policy-and-measurement only:**
  - Never holds Stripe secrets, TAO private keys, customer balances, or database credentials.
  - Never calculates customer prices or assumes provider unit rates.
  - Ignores any client-supplied `free=true` flag.
  - May only tighten (never expand) limits supplied by Server.

## 2. Day-One Default Model & Selection Contract

- New conversations default to **`provider = "chutes"`**, **`model_id = "deepseek-ai/DeepSeek-V4.1-Flash-1017"`** (`pricing_version = "chutes-catalog-observed-2026-11-22"`).
- When `selected_model` is present in `JobAssignment`, the worker locks all LLM calls to `selected_model.model_id` and rejects any attempt to smuggle another model ID.
- When `selected_model.capabilities_verified == false`, jobs requesting tools (`allowed_tools` non-empty) are rejected fail-closed until compatibility is verified.

## 3. Model Call Identifiers & Structured Events

- Every logical LLM request generates a stable `model_call_id` (`mc-<uuid>`) passed to `POST /internal/v1/inference/chat` via `X-Worker-Model-Call-ID`.
- Observed usage reports honest provenance via `usage_source`:
  - `provider_stream`: parsed from final SSE usage frame.
  - `provider_response`: parsed from non-streaming provider JSON.
  - `unknown`: provider omitted usage counts; Server reconciles independently.
- Additional structured events emitted over `POST /internal/v1/jobs/{job_id}/events`:
  - `model_call_started`
  - `model_call_finished`
  - `tool_metered`
  - `budget_warning`
  - `budget_exhausted` (latches driver closed and triggers graceful loop abort; worker cannot issue another call under a new ID to bypass caps).
- Terminal `POST /internal/v1/jobs/{job_id}/complete` includes `model_call_ids`, `tool_call_ids`, and `request_ids_seen` (never a customer price).
