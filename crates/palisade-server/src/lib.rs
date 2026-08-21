//! Palisade gRPC server library: service wiring reusable from tests and
//! embedders. See `main.rs` for the standalone binary.

mod service;

pub use service::{PalisadeService, ServiceConfig};
