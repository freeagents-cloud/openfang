# FreeAgents Internal Worker Protocol (`internal-worker.v1`)

**Canonical schema owner:** `freeagents-cloud/server/openapi/internal-worker.v1.yaml`  
**Rust client implementation:** `crates/openfang-worker/src/protocol.rs`

---

## 1. Authentication & Security Boundaries

- All calls originate **outbound** from `openfang-worker` inside a Cathedral Bittensor CPU sandbox to `https://api.freeagents.cloud`.
- Authentication uses a short-lived, single-job, single-attempt bearer token (`Authorization: Bearer <token>`), passed via `OPENFANG_JOB_TOKEN` or `--token-file` (never in logs, never persisted).
- Every request carries `x-protocol-version: 1`.
- The worker **never** receives PostgreSQL credentials, Cathedral API keys, or permanent Bittensor inference API keys.

---

## 2. Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/internal/v1/jobs/{job_id}` | Fetch the single assigned `JobAssignment` (policy, history, tools, limits). |
| `POST` | `/internal/v1/jobs/{job_id}/heartbeat` | Extend attempt lease; returns `{ "cancelled": bool, "lease_expires_at": ... }`. |
| `POST` | `/internal/v1/jobs/{job_id}/events` | Submit ordered, coalesced event batches with `(job_id, attempt, sequence)` idempotency. |
| `POST` | `/internal/v1/jobs/{job_id}/complete` | Submit terminal job status, final message, token usage, verified sources, timings, and model call telemetry. |
| `POST` | `/internal/v1/inference/chat` | OpenAI-compatible Bittensor-only inference proxy. Worker sends `x-bittensor-provider`, `x-job-id`, `x-job-attempt`, `x-trace-id`, `x-protocol-version`. |

---

## 3. Event Types & Ordering

Every event in `POST /internal/v1/jobs/{job_id}/events` has:

```json
{
  "job_id": "uuid",
  "attempt": 1,
  "sequence": 4,
  "type": "text_delta",
  "at": "2026-11-22T16:30:00Z",
  "data": { "delta": "Hello world" }
}
```

Supported `type` values:
`started`, `provider_selected`, `text_delta`, `tool_started`, `tool_finished`, `source_found`, `usage`, `checkpoint`, `completed`, `failed`, `cancelled`.

- `text_delta` frames are coalesced over a `100–500 ms` window before HTTP submission.
- Tool previews are redacted for bearer tokens, API keys, and passwords before emission.
- Hidden thinking/reasoning deltas are never emitted to public events.
- `401` / `403` / `409` / `410` on `/events` or `/heartbeat` immediately trigger local cancellation (token revoked or attempt fenced).
