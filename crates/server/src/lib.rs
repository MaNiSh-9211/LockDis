//! Palisade gRPC server library: service wiring reusable from tests and
//! embedders. See `main.rs` for the standalone binary.

mod auth;
mod service;
mod sessions;

pub use auth::{Acl, Principal};
pub use service::{PalisadeService, ServiceConfig};
pub use sessions::SessionBook;

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
