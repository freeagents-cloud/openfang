#![allow(dead_code)]

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::stream;
use openfang_worker::config::WorkerConfig;
use openfang_worker::protocol::{
    CompleteJobRequest, EventBatchRequest, HeartbeatRequest, HeartbeatResponse, JobAssignment,
    JobLimits, JobMessage, JobModelPolicy, WorkerEvent, PROTOCOL_VERSION,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use zeroize::Zeroizing;

/// Configurable behavior for the mock Server used across bounty integration tests.
#[derive(Clone, Default)]
pub struct MockServerState {
    pub jobs: Arc<Mutex<HashMap<String, JobAssignment>>>,
    pub events_received: Arc<Mutex<Vec<WorkerEvent>>>,
    pub event_batches_count: Arc<AtomicU32>,
    pub completions: Arc<Mutex<Vec<CompleteJobRequest>>>,
    pub inference_headers: Arc<Mutex<Vec<HashMap<String, String>>>>,
    pub inference_call_count: Arc<AtomicU32>,
    /// If > 0, return HTTP 500 for this many `/events` submissions before succeeding.
    pub fail_events_first_n: Arc<AtomicU32>,
    /// If > 0, return HTTP 500 for this many `/complete` submissions before succeeding.
    pub fail_complete_first_n: Arc<AtomicU32>,
    /// If true, heartbeat responds with `{ "cancelled": true }`.
    pub cancel_on_heartbeat: Arc<std::sync::atomic::AtomicBool>,
    /// If set, heartbeat returns this HTTP error status (e.g., 401 or 409).
    pub heartbeat_error_status: Arc<Mutex<Option<u16>>>,
    /// Optional artificial delay in milliseconds before responding to `/internal/v1/inference/chat`.
    pub inference_delay_ms: Arc<std::sync::atomic::AtomicU64>,
    /// Scripted SSE chunk lists per inference call index (0-based).
    pub scripted_sse_responses: Arc<Mutex<Vec<Vec<String>>>>,
    /// If > 0, return HTTP 503 for this many inference calls before succeeding (for fallback tests).
    pub fail_inference_first_n: Arc<AtomicU32>,
    /// If true, `/internal/v1/inference/chat` returns HTTP 402 `budget_exhausted`.
    pub force_budget_exhausted: Arc<std::sync::atomic::AtomicBool>,
}

impl MockServerState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert_job(&self, job: JobAssignment) {
        self.jobs
            .lock()
            .expect("jobs mutex")
            .insert(job.job_id.clone(), job);
    }

    pub fn push_text_sse_response(&self, chunks: &[&str]) {
        let mut frames = Vec::new();
        for chunk in chunks {
            frames.push(
                json!({
                    "id": "chatcmpl-mock",
                    "object": "chat.completion.chunk",
                    "model": "bittensor-test-model",
                    "choices": [{
                        "index": 0,
                        "delta": { "role": "assistant", "content": chunk },
                        "finish_reason": Value::Null
                    }]
                })
                .to_string(),
            );
        }
        frames.push(
            json!({
                "id": "chatcmpl-mock",
                "object": "chat.completion.chunk",
                "model": "bittensor-test-model",
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop"
                }],
                "usage": {
                    "prompt_tokens": 24,
                    "completion_tokens": 12,
                    "total_tokens": 36
                }
            })
            .to_string(),
        );
        frames.push("[DONE]".to_string());
        self.scripted_sse_responses
            .lock()
            .expect("sse mutex")
            .push(frames);
    }

    pub fn push_text_sse_response_without_usage(&self, chunks: &[&str]) {
        let mut frames = Vec::new();
        for chunk in chunks {
            frames.push(
                json!({
                    "id": "chatcmpl-no-usage",
                    "object": "chat.completion.chunk",
                    "model": "deepseek-ai/DeepSeek-V4.1-Flash-1017",
                    "choices": [{
                        "index": 0,
                        "delta": { "role": "assistant", "content": chunk },
                        "finish_reason": Value::Null
                    }]
                })
                .to_string(),
            );
        }
        frames.push(
            json!({
                "id": "chatcmpl-no-usage",
                "object": "chat.completion.chunk",
                "model": "deepseek-ai/DeepSeek-V4.1-Flash-1017",
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "stop"
                }]
            })
            .to_string(),
        );
        frames.push("[DONE]".to_string());
        self.scripted_sse_responses
            .lock()
            .expect("sse mutex")
            .push(frames);
    }

    pub fn push_tool_call_sse_response(&self, call_id: &str, tool_name: &str, args_json: &str) {
        let frames = vec![
            json!({
                "id": "chatcmpl-tool",
                "object": "chat.completion.chunk",
                "model": "bittensor-test-model",
                "choices": [{
                    "index": 0,
                    "delta": {
                        "role": "assistant",
                        "tool_calls": [{
                            "index": 0,
                            "id": call_id,
                            "type": "function",
                            "function": {
                                "name": tool_name,
                                "arguments": args_json
                            }
                        }]
                    },
                    "finish_reason": Value::Null
                }]
            })
            .to_string(),
            json!({
                "id": "chatcmpl-tool",
                "object": "chat.completion.chunk",
                "model": "bittensor-test-model",
                "choices": [{
                    "index": 0,
                    "delta": {},
                    "finish_reason": "tool_calls"
                }],
                "usage": {
                    "prompt_tokens": 30,
                    "completion_tokens": 15,
                    "total_tokens": 45
                }
            })
            .to_string(),
            "[DONE]".to_string(),
        ];
        self.scripted_sse_responses
            .lock()
            .expect("sse mutex")
            .push(frames);
    }
}

pub async fn start_mock_server(state: MockServerState) -> (String, tokio::task::JoinHandle<()>) {
    let app = Router::new()
        .route("/internal/v1/jobs/{job_id}", get(handle_get_job))
        .route("/internal/v1/jobs/{job_id}/events", post(handle_events))
        .route("/internal/v1/jobs/{job_id}/heartbeat", post(handle_heartbeat))
        .route("/internal/v1/jobs/{job_id}/complete", post(handle_complete))
        .route("/internal/v1/inference/chat", post(handle_inference))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("local addr");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), handle)
}

async fn handle_get_job(
    State(state): State<MockServerState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    let guard = state.jobs.lock().expect("jobs mutex");
    match guard.get(&job_id) {
        Some(job) => (StatusCode::OK, Json(job.clone())).into_response(),
        None => (StatusCode::NOT_FOUND, "job not found").into_response(),
    }
}

async fn handle_events(
    State(state): State<MockServerState>,
    Path(_job_id): Path<String>,
    Json(batch): Json<EventBatchRequest>,
) -> impl IntoResponse {
    let fail_rem = state.fail_events_first_n.load(Ordering::SeqCst);
    if fail_rem > 0 {
        state.fail_events_first_n.fetch_sub(1, Ordering::SeqCst);
        return (StatusCode::INTERNAL_SERVER_ERROR, "transient error").into_response();
    }
    state.event_batches_count.fetch_add(1, Ordering::SeqCst);
    state
        .events_received
        .lock()
        .expect("events mutex")
        .extend(batch.events);
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

async fn handle_heartbeat(
    State(state): State<MockServerState>,
    Path(_job_id): Path<String>,
    Json(_req): Json<HeartbeatRequest>,
) -> impl IntoResponse {
    if let Some(code) = *state.heartbeat_error_status.lock().expect("hb err") {
        let status = StatusCode::from_u16(code).unwrap_or(StatusCode::UNAUTHORIZED);
        return (status, "revoked").into_response();
    }
    let cancelled = state.cancel_on_heartbeat.load(Ordering::SeqCst);
    (
        StatusCode::OK,
        Json(HeartbeatResponse {
            cancelled,
            lease_expires_at: None,
        }),
    )
        .into_response()
}

async fn handle_complete(
    State(state): State<MockServerState>,
    Path(_job_id): Path<String>,
    Json(req): Json<CompleteJobRequest>,
) -> impl IntoResponse {
    let fail_rem = state.fail_complete_first_n.load(Ordering::SeqCst);
    if fail_rem > 0 {
        state.fail_complete_first_n.fetch_sub(1, Ordering::SeqCst);
        return (StatusCode::INTERNAL_SERVER_ERROR, "transient complete error").into_response();
    }
    state.completions.lock().expect("completions").push(req);
    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

async fn handle_inference(
    State(state): State<MockServerState>,
    headers: HeaderMap,
    Json(_body): Json<Value>,
) -> impl IntoResponse {
    let mut recorded = HashMap::new();
    for (k, v) in &headers {
        if let Ok(s) = v.to_str() {
            recorded.insert(k.as_str().to_string(), s.to_string());
        }
    }
    state
        .inference_headers
        .lock()
        .expect("headers mutex")
        .push(recorded);

    let delay_ms = state.inference_delay_ms.load(Ordering::SeqCst);
    if delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
    }

    if state.force_budget_exhausted.load(Ordering::SeqCst) {
        return (
            StatusCode::PAYMENT_REQUIRED,
            Json(json!({ "error": { "code": "budget_exhausted", "message": "budget_exhausted: job provider cost cap reached" } })),
        )
            .into_response();
    }

    let fail_rem = state.fail_inference_first_n.load(Ordering::SeqCst);
    if fail_rem > 0 {
        state.fail_inference_first_n.fetch_sub(1, Ordering::SeqCst);
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": { "message": "upstream engy temporarily overloaded" } })),
        )
            .into_response();
    }

    let call_idx = state.inference_call_count.fetch_add(1, Ordering::SeqCst) as usize;
    let frames = {
        let guard = state.scripted_sse_responses.lock().expect("sse mutex");
        guard
            .get(call_idx)
            .or_else(|| guard.last())
            .cloned()
            .unwrap_or_else(|| {
                vec![
                    json!({
                        "id": "chatcmpl-default",
                        "object": "chat.completion.chunk",
                        "model": "bittensor-test-model",
                        "choices": [{
                            "index": 0,
                            "delta": { "role": "assistant", "content": "Default mock answer." },
                            "finish_reason": Value::Null
                        }]
                    })
                    .to_string(),
                    json!({
                        "id": "chatcmpl-default",
                        "object": "chat.completion.chunk",
                        "model": "bittensor-test-model",
                        "choices": [{
                            "index": 0,
                            "delta": {},
                            "finish_reason": "stop"
                        }],
                        "usage": { "prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15 }
                    })
                    .to_string(),
                    "[DONE]".to_string(),
                ]
            })
    };

    let sse_stream = stream::iter(
        frames
            .into_iter()
            .map(|data| Ok::<Event, Infallible>(Event::default().data(data))),
    );
    Sse::new(sse_stream)
        .keep_alive(KeepAlive::default())
        .into_response()
}

pub fn sample_job(job_id: &str, prompt: &str) -> JobAssignment {
    JobAssignment {
        protocol_version: PROTOCOL_VERSION,
        job_id: job_id.to_string(),
        attempt: 1,
        conversation_id: format!("conv-{job_id}"),
        messages: vec![JobMessage {
            role: "user".to_string(),
            content: prompt.to_string(),
        }],
        system_policy_version: "bounty-v1".to_string(),
        model_policy: JobModelPolicy {
            allowed_providers: vec!["engy".into(), "chutes".into(), "saygm".into()],
            preferred: Some("engy".into()),
        },
        allowed_tools: vec!["web_search".into(), "web_fetch".into()],
        limits: JobLimits {
            max_agent_turns: 4,
            max_tokens: 12_000,
            deadline_ms: 15_000,
            max_output_bytes: 65_536,
        },
        trace_id: format!("trace-{job_id}"),
        model: Some("bittensor-test-model".to_string()),
        billing_context: None,
        selected_model: None,
    }
}

pub fn sample_worker_config(server_url: String, token: &str) -> WorkerConfig {
    WorkerConfig {
        server_url,
        job_token: Zeroizing::new(token.to_string()),
        manifest_path: PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../agents/freeagents-bounty.toml"),
        event_flush_interval_ms: 50,
        event_max_batch_size: 16,
        heartbeat_interval_ms: 200,
        default_model: "bittensor-test-model".to_string(),
        web_search_provider: "duckduckgo".to_string(),
        allow_insecure_http: true,
    }
}
