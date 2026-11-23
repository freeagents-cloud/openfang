//! Core bounded job executor wiring `openfang-runtime::agent_loop::run_agent_loop_streaming`
//! to `openfang-bittensor` and the Server `internal-worker.v1` protocol.

use crate::cancellation::{CancelReason, JobCancellation};
use crate::config::WorkerConfig;
use crate::events::{redact_sensitive, spawn_event_batcher, EventBatcherConfig};
use crate::fetch_job::fetch_job;
use crate::heartbeat::{spawn_heartbeat, HeartbeatConfig};
use crate::protocol::{
    CompleteJobRequest, JobAssignment, JobTimings, SourceRecord, TerminalStatus, UsageSummary,
    WorkerEventType, PROTOCOL_VERSION,
};
use crate::session::EphemeralJobState;
use crate::shutdown::spawn_signal_watcher;
use anyhow::Result;
use chrono::Utc;
use openfang_bittensor::{BittensorProxyConfig, BittensorProxyDriver, MeteringCallbacks};
use openfang_runtime::agent_loop::run_agent_loop_streaming;
use openfang_runtime::llm_driver::{LlmDriver, StreamEvent};
use openfang_runtime::tool_runner::builtin_tool_definitions;
use openfang_runtime::web_cache::WebCache;
use openfang_runtime::web_fetch::WebFetchEngine;
use openfang_runtime::web_search::{WebSearchEngine, WebToolsContext};
use openfang_types::config::{SearchProvider, WebConfig, WebFetchConfig};
use regex_lite::Regex;
use serde_json::json;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

const MAX_COMPLETE_RETRIES: u32 = 4;

/// Final outcome returned by [`run_single_job`].
#[derive(Debug)]
pub struct JobRunOutcome {
    pub job_id: String,
    pub attempt: u32,
    pub status: TerminalStatus,
    pub finish_reason: String,
    pub message: Option<String>,
    pub sources: Vec<SourceRecord>,
}

/// Fetch and execute one assigned job, streaming events and submitting terminal completion.
pub async fn run_single_job(config: &WorkerConfig, job_id: &str) -> Result<JobRunOutcome> {
    config.validate()?;

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;

    let job = fetch_job(&client, &config.server_url, job_id, config.job_token.as_str()).await?;
    execute_job_assignment(config, client, job, JobCancellation::new()).await
}

/// Execute an already-fetched [`JobAssignment`] with an injected [`JobCancellation`] handle.
///
/// Exposed publicly so integration tests can drive cancellation and concurrency directly.
pub async fn execute_job_assignment(
    config: &WorkerConfig,
    client: reqwest::Client,
    job: JobAssignment,
    cancel: JobCancellation,
) -> Result<JobRunOutcome> {
    let started_at = Utc::now();

    // Fail-closed policy + billing_context + selected_model validation BEFORE starting loop.
    let resolved = match job.validate_and_resolve(&config.default_model) {
        Ok(r) => r,
        Err(reason) => {
            error!(job_id = %job.job_id, error = %reason, "Rejected job due to policy validation failure");
            let complete = CompleteJobRequest {
                protocol_version: PROTOCOL_VERSION,
                job_id: job.job_id.clone(),
                attempt: job.attempt,
                trace_id: job.trace_id.clone(),
                status: TerminalStatus::Failed,
                message: None,
                usage: UsageSummary::default(),
                provider: None,
                model: job
                    .selected_model
                    .as_ref()
                    .map(|s| s.model_id.clone())
                    .or_else(|| job.model.clone()),
                finish_reason: "policy_rejected".to_string(),
                timings: JobTimings {
                    started_at,
                    finished_at: Utc::now(),
                    duration_ms: 0,
                },
                sources: Vec::new(),
                model_calls: Vec::new(),
                model_call_ids: Vec::new(),
                tool_call_ids: Vec::new(),
                request_ids_seen: Vec::new(),
                error: Some(reason.clone()),
            };
            let _ = submit_completion(&client, &config.server_url, config.job_token.as_str(), &complete).await;
            return Ok(JobRunOutcome {
                job_id: job.job_id,
                attempt: job.attempt,
                status: TerminalStatus::Failed,
                finish_reason: "policy_rejected".to_string(),
                message: None,
                sources: Vec::new(),
            });
        }
    };

    let policy = resolved.model_policy.clone();
    let max_tool_calls = resolved.max_tool_calls;
    let locked_model_id = resolved.selected_model_id.clone();

    let mut state = EphemeralJobState::build(&job, &config.manifest_path, &locked_model_id)?;

    let (emitter, batcher_handle) = spawn_event_batcher(EventBatcherConfig {
        client: client.clone(),
        server_url: config.server_url.clone(),
        job_id: job.job_id.clone(),
        attempt: job.attempt,
        trace_id: job.trace_id.clone(),
        job_token: config.job_token.as_str().to_string(),
        flush_interval: Duration::from_millis(config.event_flush_interval_ms),
        max_batch_size: config.event_max_batch_size,
        cancel: cancel.clone(),
    });

    let heartbeat_handle = spawn_heartbeat(HeartbeatConfig {
        client: client.clone(),
        server_url: config.server_url.clone(),
        job_id: job.job_id.clone(),
        attempt: job.attempt,
        trace_id: job.trace_id.clone(),
        job_token: config.job_token.as_str().to_string(),
        interval: Duration::from_millis(config.heartbeat_interval_ms),
        cancel: cancel.clone(),
    });

    let signal_handle = spawn_signal_watcher(cancel.clone());

    let selected_provider: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let selected_model: Arc<Mutex<Option<String>>> =
        Arc::new(Mutex::new(Some(state.manifest.model.model.clone())));

    let provider_emitter = emitter.clone();
    let provider_slot = Arc::clone(&selected_provider);
    let model_slot = Arc::clone(&selected_model);

    let started_emitter = emitter.clone();
    let finished_emitter = emitter.clone();
    let budget_emitter = emitter.clone();
    let budget_cancel = cancel.clone();

    let proxy_driver = BittensorProxyDriver::new(BittensorProxyConfig {
        server_base_url: config.server_url.clone(),
        job_token: config.job_token.clone(),
        job_id: job.job_id.clone(),
        attempt: job.attempt,
        trace_id: job.trace_id.clone(),
        policy,
        locked_model_id: Some(locked_model_id.clone()),
    })?
    .with_on_provider_selected(Arc::new(move |provider, model| {
        *provider_slot.lock().expect("provider mutex") = Some(provider.to_string());
        *model_slot.lock().expect("model mutex") = Some(model.to_string());
        provider_emitter.emit(
            WorkerEventType::ProviderSelected,
            json!({
                "provider": provider.as_str(),
                "model": model,
            }),
        );
    }))
    .with_metering_callbacks(MeteringCallbacks {
        on_started: Some(Arc::new(move |ev| {
            started_emitter.emit(
                WorkerEventType::ModelCallStarted,
                serde_json::to_value(ev).unwrap_or_else(|_| json!({})),
            );
        })),
        on_finished: Some(Arc::new(move |ev| {
            finished_emitter.emit(
                WorkerEventType::ModelCallFinished,
                serde_json::to_value(ev).unwrap_or_else(|_| json!({})),
            );
        })),
        on_budget_exhausted: Some(Arc::new(move |limit_kind, observed_value, cap| {
            budget_emitter.emit(
                WorkerEventType::BudgetExhausted,
                json!({
                    "limit_kind": limit_kind,
                    "observed_value": observed_value,
                    "cap": cap,
                    "safe_abort": true,
                }),
            );
            budget_cancel.cancel(CancelReason::BudgetExhausted);
        })),
    });

    let telemetry = proxy_driver.telemetry();
    let driver: Arc<dyn LlmDriver> = Arc::new(proxy_driver);

    // Filter upstream builtin tools strictly to allowed bounty read-only tools.
    let allowed_tool_set: HashSet<&str> = state
        .manifest
        .capabilities
        .tools
        .iter()
        .map(String::as_str)
        .collect();
    let available_tools: Vec<_> = builtin_tool_definitions()
        .into_iter()
        .filter(|t| allowed_tool_set.contains(t.name.as_str()))
        .collect();

    let web_ctx = build_web_tools_context(&config.web_search_provider);

    emitter.emit(
        WorkerEventType::Started,
        json!({
            "conversation_id": job.conversation_id,
            "system_policy_version": job.system_policy_version,
            "allowed_tools": state.manifest.capabilities.tools,
            "max_agent_turns": job.limits.max_agent_turns,
        }),
    );

    // Bridge OpenFang's `StreamEvent` channel to our coalescing `EventEmitter`
    // and collect verified `SourceRecord`s from real tool executions.
    let (stream_tx, mut stream_rx) = mpsc::channel::<StreamEvent>(128);
    let stream_emitter = emitter.clone();
    let sources_store: Arc<Mutex<Vec<SourceRecord>>> = Arc::new(Mutex::new(Vec::new()));
    let sources_for_stream = Arc::clone(&sources_store);
    let tool_call_ids_store: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let tool_ids_for_stream = Arc::clone(&tool_call_ids_store);
    let tool_cancel = cancel.clone();
    let max_output_bytes = job.limits.max_output_bytes;

    let stream_consumer = tokio::spawn(async move {
        let mut emitted_bytes: usize = 0;
        let mut tool_calls_observed: u32 = 0;
        let mut seen_urls: HashSet<String> = HashSet::new();
        let mut pending_fetch_urls: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        let mut tool_started_at: std::collections::HashMap<String, chrono::DateTime<Utc>> =
            std::collections::HashMap::new();

        while let Some(event) = stream_rx.recv().await {
            match event {
                StreamEvent::TextDelta { text } => {
                    if emitted_bytes < max_output_bytes {
                        let remaining = max_output_bytes - emitted_bytes;
                        let slice = truncate_utf8(&text, remaining);
                        emitted_bytes += slice.len();
                        stream_emitter.emit_text_delta(slice);
                    }
                }
                StreamEvent::ToolUseStart { id, name } => {
                    tool_started_at.insert(id.clone(), Utc::now());
                    stream_emitter.emit(
                        WorkerEventType::ToolStarted,
                        json!({
                            "tool_call_id": id,
                            "tool": name,
                        }),
                    );
                }
                StreamEvent::ToolUseEnd { id, name, input } => {
                    if name == "web_fetch" {
                        if let Some(url) = input.get("url").and_then(|v| v.as_str()) {
                            pending_fetch_urls.insert(id, url.to_string());
                        }
                    }
                }
                StreamEvent::ToolExecutionResult {
                    id,
                    name,
                    result_preview,
                    is_error,
                } => {
                    let ended_at = Utc::now();
                    let started_at_ts = tool_started_at.remove(&id).unwrap_or(ended_at);
                    tool_calls_observed = tool_calls_observed.saturating_add(1);
                    tool_ids_for_stream
                        .lock()
                        .expect("tool_ids mutex")
                        .push(id.clone());

                    let safe_preview = redact_sensitive(&result_preview);
                    stream_emitter.emit(
                        WorkerEventType::ToolFinished,
                        json!({
                            "tool_call_id": id,
                            "tool": name,
                            "is_error": is_error,
                            "preview": truncate_utf8(&safe_preview, 512),
                        }),
                    );
                    stream_emitter.emit(
                        WorkerEventType::ToolMetered,
                        json!({
                            "tool_call_id": id,
                            "tool_kind": name,
                            "started_at": started_at_ts,
                            "ended_at": ended_at,
                            "success": !is_error,
                        }),
                    );

                    if max_tool_calls > 0 && tool_calls_observed >= max_tool_calls {
                        stream_emitter.emit(
                            WorkerEventType::BudgetExhausted,
                            json!({
                                "limit_kind": "max_tool_calls",
                                "observed_value": tool_calls_observed,
                                "cap": max_tool_calls,
                                "safe_abort": true,
                            }),
                        );
                        tool_cancel.cancel(CancelReason::BudgetExhausted);
                    }

                    if !is_error && matches!(name.as_str(), "web_search" | "web_fetch") {
                        let mut extracted = extract_urls(&result_preview);
                        if let Some(fetched) = pending_fetch_urls.remove(&id) {
                            if !extracted.contains(&fetched) {
                                extracted.insert(0, fetched);
                            }
                        }
                        for url in extracted {
                            if seen_urls.insert(url.clone()) {
                                let mut guard =
                                    sources_for_stream.lock().expect("sources mutex poisoned");
                                let source_id = format!("s{}", guard.len() + 1);
                                let rec = SourceRecord {
                                    source_id: source_id.clone(),
                                    url: url.clone(),
                                    title: None,
                                    retrieved_at: Utc::now(),
                                    tool: name.clone(),
                                };
                                guard.push(rec.clone());
                                drop(guard);
                                stream_emitter.emit(
                                    WorkerEventType::SourceFound,
                                    json!({
                                        "source_id": rec.source_id,
                                        "url": rec.url,
                                        "title": rec.title,
                                        "retrieved_at": rec.retrieved_at,
                                        "tool": rec.tool,
                                    }),
                                );
                            }
                        }
                    }
                }
                StreamEvent::ContentComplete { usage, stop_reason } => {
                    stream_emitter.emit(
                        WorkerEventType::Usage,
                        json!({
                            "input_tokens": usage.input_tokens,
                            "output_tokens": usage.output_tokens,
                            "stop_reason": format!("{stop_reason:?}"),
                        }),
                    );
                }
                StreamEvent::PhaseChange { phase, detail } => {
                    stream_emitter.emit(
                        WorkerEventType::Checkpoint,
                        json!({
                            "phase": phase,
                            "detail": detail,
                        }),
                    );
                }
                // Never leak hidden reasoning/thinking text to public events.
                StreamEvent::ThinkingDelta { .. } | StreamEvent::ToolInputDelta { .. } => {}
            }
        }
    });

    let deadline = Duration::from_millis(job.limits.deadline_ms);
    let cancel_token = cancel.token();

    let loop_future = run_agent_loop_streaming(
        &state.manifest,
        &state.latest_user_message,
        &mut state.session,
        &state.memory,
        driver,
        &available_tools,
        None, // kernel: disabled in bounty mode
        stream_tx,
        None, // skill_registry: disabled
        None, // mcp_connections: disabled
        Some(&web_ctx),
        None, // browser_ctx: disabled
        None, // embedding_driver: disabled
        None, // workspace_root: disabled
        None, // on_phase: stream_tx already emits PhaseChange
        None, // media_engine: disabled
        None, // tts_engine: disabled
        None, // docker_config: disabled
        None, // hooks: disabled
        None, // context_window_tokens
        None, // process_manager: disabled
        None, // user_content_blocks
    );

    let loop_outcome = tokio::select! {
        res = loop_future => {
            Some(res)
        }
        _ = cancel_token.cancelled() => {
            warn!(job_id = %job.job_id, reason = ?cancel.reason(), "Agent loop cancelled");
            None
        }
        _ = tokio::time::sleep(deadline) => {
            warn!(job_id = %job.job_id, deadline_ms = job.limits.deadline_ms, "Agent loop hit deadline");
            cancel.cancel(CancelReason::DeadlineExceeded);
            None
        }
    };

    // Wait for stream consumer to drain any remaining buffered StreamEvents.
    let _ = stream_consumer.await;

    let finished_at = Utc::now();
    let duration_ms = (finished_at - started_at).num_milliseconds().max(0) as u64;
    let (input_tokens, output_tokens) = telemetry.totals();
    let model_calls = telemetry.snapshot();
    let model_call_ids: Vec<String> = model_calls
        .iter()
        .map(|mc| mc.model_call_id.clone())
        .collect();
    let request_ids_seen: Vec<String> = model_calls
        .iter()
        .filter_map(|mc| mc.provider_request_id.clone())
        .collect();
    let tool_call_ids = tool_call_ids_store.lock().expect("tool_ids mutex").clone();
    let sources = sources_store.lock().expect("sources mutex").clone();
    let provider = selected_provider.lock().expect("provider mutex").clone();
    let model = selected_model.lock().expect("model mutex").clone();

    let (status, finish_reason, message, iterations, error_msg) = match loop_outcome {
        Some(Ok(result)) => {
            let final_msg = truncate_utf8(&result.response, max_output_bytes).to_string();
            emitter.emit(
                WorkerEventType::Completed,
                json!({
                    "finish_reason": "stop",
                    "iterations": result.iterations,
                    "provider": provider,
                    "model": model,
                }),
            );
            (
                TerminalStatus::Completed,
                "stop".to_string(),
                Some(final_msg),
                result.iterations,
                None,
            )
        }
        Some(Err(err)) => {
            let msg = err.to_string();
            emitter.emit(
                WorkerEventType::Failed,
                json!({
                    "error": msg,
                }),
            );
            (
                TerminalStatus::Failed,
                "error".to_string(),
                None,
                0,
                Some(msg),
            )
        }
        None => {
            let reason = cancel
                .reason()
                .map(|r| r.as_str().to_string())
                .unwrap_or_else(|| "cancelled".to_string());
            emitter.emit(
                WorkerEventType::Cancelled,
                json!({
                    "reason": reason,
                }),
            );
            (
                TerminalStatus::Cancelled,
                reason.clone(),
                None,
                0,
                Some(format!("job cancelled: {reason}")),
            )
        }
    };

    // Stop background heartbeat & signal watcher, then flush ALL pending events before complete.
    cancel.cancel(CancelReason::ServerCancelled);
    heartbeat_handle.abort();
    signal_handle.abort();
    drop(emitter);
    let _ = batcher_handle.await;

    let complete_req = CompleteJobRequest {
        protocol_version: PROTOCOL_VERSION,
        job_id: job.job_id.clone(),
        attempt: job.attempt,
        trace_id: job.trace_id.clone(),
        status,
        message: message.clone(),
        usage: UsageSummary {
            input_tokens,
            output_tokens,
            total_tokens: input_tokens.saturating_add(output_tokens),
            model_calls: model_calls.len() as u32,
            iterations,
        },
        provider,
        model,
        finish_reason: finish_reason.clone(),
        timings: JobTimings {
            started_at,
            finished_at,
            duration_ms,
        },
        sources: sources.clone(),
        model_calls,
        model_call_ids,
        tool_call_ids,
        request_ids_seen,
        error: error_msg,
    };

    if let Err(err) = submit_completion(
        &client,
        &config.server_url,
        config.job_token.as_str(),
        &complete_req,
    )
    .await
    {
        warn!(job_id = %job.job_id, error = %err, "Failed to submit terminal completion to Server");
    }

    info!(
        job_id = %job.job_id,
        attempt = job.attempt,
        status = ?status,
        duration_ms,
        "Worker finished job attempt"
    );

    // `state` (including TempDir and in-memory SQLite) is dropped and cleaned up here.
    drop(state);

    Ok(JobRunOutcome {
        job_id: job.job_id,
        attempt: job.attempt,
        status,
        finish_reason,
        message,
        sources,
    })
}

fn build_web_tools_context(provider_name: &str) -> WebToolsContext {
    let search_provider = match provider_name.trim().to_ascii_lowercase().as_str() {
        "searxng" => SearchProvider::Searxng,
        _ => SearchProvider::DuckDuckGo,
    };
    let web_cfg = WebConfig {
        search_provider,
        ..WebConfig::default()
    };
    let fetch_cfg = WebFetchConfig::default();
    let cache = Arc::new(WebCache::new(Duration::from_secs(300)));
    WebToolsContext {
        search: WebSearchEngine::new(web_cfg, Arc::clone(&cache)),
        fetch: WebFetchEngine::new(fetch_cfg, cache),
    }
}

async fn submit_completion(
    client: &reqwest::Client,
    server_url: &str,
    job_token: &str,
    payload: &CompleteJobRequest,
) -> Result<()> {
    let url = format!(
        "{}/internal/v1/jobs/{}/complete",
        server_url.trim_end_matches('/'),
        payload.job_id
    );
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=MAX_COMPLETE_RETRIES {
        match client
            .post(&url)
            .bearer_auth(job_token)
            .header("x-protocol-version", PROTOCOL_VERSION.to_string())
            .json(payload)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => return Ok(()),
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                let err = anyhow::anyhow!("POST {url} returned HTTP {status}: {body}");
                if status.is_client_error() {
                    return Err(err);
                }
                last_err = Some(err);
            }
            Err(e) => {
                last_err = Some(e.into());
            }
        }
        if attempt < MAX_COMPLETE_RETRIES {
            tokio::time::sleep(Duration::from_millis(200 * u64::from(attempt))).await;
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("failed to submit job completion")))
}

fn extract_urls(text: &str) -> Vec<String> {
    let Ok(re) = Regex::new(r#"https?://[^\s<>"'\)\]]+"#) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for m in re.find_iter(text) {
        let cleaned = m
            .as_str()
            .trim_end_matches(['.', ',', ';', ':', ')']);
        if !cleaned.is_empty() && !out.iter().any(|u| u == cleaned) {
            out.push(cleaned.to_string());
        }
    }
    out
}

fn truncate_utf8(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut idx = max_bytes;
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    &s[..idx]
}
