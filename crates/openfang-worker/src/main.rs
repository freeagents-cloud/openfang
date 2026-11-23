//! `openfang-worker` binary entrypoint.
//!
//! Modes:
//! - `openfang-worker run --job-id <uuid> --server <url>`: Cathedral single-job mode (MVP).
//! - `openfang-worker serve --server <url>`: Reserved for long-running ms.computer warm workers.

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};
use openfang_worker::config::{load_job_token, WorkerConfig, WorkerFileConfig};
use openfang_worker::protocol::TerminalStatus;
use openfang_worker::runner::run_single_job;
use std::path::PathBuf;
use std::process::ExitCode;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "openfang-worker",
    version,
    about = "Stateless FreeAgents OpenFang worker for Bittensor CPU sandboxes"
)]
struct Cli {
    /// Optional path to a TOML worker config file.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Path to the bounty agent manifest (defaults to agents/freeagents-bounty.toml).
    #[arg(long, global = true)]
    manifest: Option<PathBuf>,

    /// Path to a file containing the short-lived job token (alternative to OPENFANG_JOB_TOKEN).
    #[arg(long, global = true)]
    token_file: Option<PathBuf>,

    /// Allow insecure http:// Server URLs (for local development only).
    #[arg(long, global = true, default_value_t = false)]
    allow_insecure_http: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Execute a single assigned job from Server and exit.
    Run {
        /// Assigned job UUID.
        #[arg(long)]
        job_id: String,

        /// Control-plane Server base URL (e.g. https://api.freeagents.cloud).
        #[arg(long)]
        server: String,

        /// Optional model override if Server omits model ID in job payload.
        #[arg(long)]
        default_model: Option<String>,
    },
    /// Long-running poll-loop mode (deferred until ms.computer deployment interface is validated).
    Serve {
        /// Control-plane Server base URL.
        #[arg(long)]
        server: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .with_target(true)
        .init();

    match run_cli().await {
        Ok(code) => code,
        Err(err) => {
            tracing::error!(error = %err, "openfang-worker exited with error");
            ExitCode::FAILURE
        }
    }
}

async fn run_cli() -> Result<ExitCode> {
    let cli = Cli::parse();
    let file_cfg = match cli.config.as_deref() {
        Some(path) => WorkerFileConfig::load(path)?,
        None => WorkerFileConfig::default(),
    };

    match cli.command {
        Commands::Run {
            job_id,
            server,
            default_model,
        } => {
            let job_token = load_job_token(cli.token_file.as_deref())?;
            let config = WorkerConfig {
                server_url: server,
                job_token,
                manifest_path: cli
                    .manifest
                    .or(file_cfg.manifest_path)
                    .unwrap_or_else(|| PathBuf::from("/opt/openfang/agents/freeagents-bounty.toml")),
                event_flush_interval_ms: file_cfg.event_flush_interval_ms.unwrap_or(150),
                event_max_batch_size: file_cfg.event_max_batch_size.unwrap_or(32),
                heartbeat_interval_ms: file_cfg.heartbeat_interval_ms.unwrap_or(15_000),
                default_model: default_model
                    .or(file_cfg.default_model)
                    .unwrap_or_else(|| "default".to_string()),
                web_search_provider: file_cfg
                    .web_search_provider
                    .unwrap_or_else(|| "duckduckgo".to_string()),
                allow_insecure_http: cli.allow_insecure_http,
            };

            let outcome = run_single_job(&config, &job_id).await?;
            match outcome.status {
                TerminalStatus::Completed => Ok(ExitCode::SUCCESS),
                TerminalStatus::Cancelled | TerminalStatus::Failed => Ok(ExitCode::FAILURE),
            }
        }
        Commands::Serve { server: _ } => {
            bail!(
                "`openfang-worker serve` is gated pending ms.computer deployment interface validation (Spec §4.1 & §7); use `openfang-worker run --job-id <uuid> --server <url>`"
            );
        }
    }
}
