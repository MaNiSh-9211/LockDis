//! Redis backend for Palisade.
//!
//! Implements the [`palisade_core`] locking traits over Redis:
//! single-instance leases with Lua-guarded release/extend, mandatory
//! fencing tokens, watchdog auto-renewal, and Redlock quorum mode.
//!
//! Status: Phase 1 — single-instance mutex complete; watchdog, RWL,
//! semaphore, fair queues, and Redlock land next.

pub mod config;
mod scripts;
pub mod single;

pub use config::RedisConfig;
pub use single::{RedisLockHandle, RedisLockManager};
