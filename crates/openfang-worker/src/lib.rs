//! FreeAgents stateless OpenFang worker library.
//!
//! Executes a single bounded job inside a Bittensor CPU sandbox using the real
//! upstream `openfang-runtime` agent loop (`run_agent_loop_streaming`),
//! `openfang-bittensor`'s fail-closed proxy driver, and ephemeral per-job state.

pub mod billing_policy;
pub mod cancellation;
pub mod config;
pub mod events;
pub mod fetch_job;
pub mod heartbeat;
pub mod protocol;
pub mod runner;
pub mod session;
pub mod shutdown;
