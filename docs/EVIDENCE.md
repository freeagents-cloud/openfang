# FreeAgents Bounty — Execution Evidence & Receipts

This document describes what provenance data `openfang-worker` records for each job attempt and how it combines with Server-collected Cathedral receipts.

## 1. What `openfang-worker` Reports on Completion

On every `POST /internal/v1/jobs/{job_id}/complete`, the worker submits:

- **Immutable job identity:** `job_id`, `attempt`, `trace_id`, `protocol_version`.
- **Per-call Bittensor inference records (`model_calls`):**
  - `provider` (`engy`, `chutes`, or `saygm` — enforced fail-closed)
  - `model_id`
  - `provider_request_id` (when supplied by proxy/provider)
  - `inference_proof` (when supported by Engy)
  - `started_at`, `finished_at`
  - `input_tokens`, `output_tokens`
  - `error` (if a call failed)
  - `trace_id`
- **Verified retrieved sources (`sources`):** URLs actually returned by `web_search` / `web_fetch` tool executions with retrieval timestamps (never fabricated from LLM prose).
- **Timings & usage:** `started_at`, `finished_at`, `duration_ms`, total tokens, model call count, and agent loop iterations.

## 2. Server-Side Compute Provenance (Cathedral)

When Server dispatches `openfang-worker` inside a Cathedral confidential sandbox (`amd_sev_snp`), Server pairs the worker's completion record with:
- Pinned OCI image digest (`sha256:...`) built from `docker/Dockerfile.worker`
- Worker build commit SHA (`git rev-parse HEAD`)
- Cathedral `sandbox_id`, hardware class (`amd_sev_snp`), and signed Cathedral receipts

## 3. Truthfulness Boundaries

- Cathedral receipts attest to platform-described execution and hardware properties, **not** that the model's answer is factually correct.
- Generic HTTP metadata from an inference provider is **not** cryptographic proof of model weights or decentralization; only explicitly verified proofs (e.g., Engy inference proof where available) should be labeled as verified proofs.
- Public bounty demonstrations must use synthetic or explicitly public prompts and never expose private user conversations.
