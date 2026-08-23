//! Palisade gRPC server library: service wiring reusable from tests and
//! embedders. See `main.rs` for the standalone binary.

mod auth;
mod registry;
mod service;
mod sessions;
mod watch_hub;

pub use auth::{Acl, AuthMode, Principal};
pub use registry::HeldRegistry;
pub use service::{PalisadeService, ServiceConfig};
pub use sessions::{HbResult, SessionBook};
pub use watch_hub::WatchHub;

use std::sync::Arc;

/// Starts the session sweeper for a service; call once at startup.
pub fn start_session_sweeper(service: &PalisadeService) -> tokio::task::JoinHandle<()> {
    sessions::spawn_sweeper(service.session_book())
}

/// Convenience wrapper used by `main`.
#[allow(dead_code)]
pub(crate) fn sweeper_for(book: Arc<SessionBook>) -> tokio::task::JoinHandle<()> {
    sessions::spawn_sweeper(book)
}
