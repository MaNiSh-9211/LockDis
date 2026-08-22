//! Palisade core: backend-neutral types, traits, and fencing primitives.
//!
//! This crate performs no I/O. Storage backends (e.g. [`palisade_redis`])
//! implement the locking traits defined here.

mod error;
mod fence_pg;
mod fencing;
mod lock;
mod owner;

pub use error::{Error, Result};
pub use fence_pg::{
    ensure_fence_column as pg_ensure_fence_column, fenced_select as pg_fenced_select,
    fenced_update as pg_fenced_update,
};
pub use fencing::FencingToken;
pub use lock::{LockHandle, LockManager, LockOptions, MIN_TTL};
pub use owner::OwnerId;
