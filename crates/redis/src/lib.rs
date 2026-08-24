//! Redis backend for Palisade.
//!
//! Implements the [`palisade_core`] locking traits over Redis:
//! single-instance leases with Lua-guarded release/extend, mandatory
//! fencing tokens, watchdog auto-renewal, and Redlock quorum mode.
//!
//! Status: Phases 1–3 complete (mutex, fencing, watchdog, reentrant,
//! read-write, semaphore, fair queue, multi-lock). Redlock lands in
//! Phase 4.

pub mod config;
pub mod fair;
pub mod latch;
pub mod multi;
pub mod redlock;
pub mod reentrant;
pub mod rwlock;
mod scripts;
pub mod semaphore;
pub mod single;
pub mod testament;

pub use config::RedisConfig;
pub use fair::FairLockHandle;
pub use latch::RedisCountDownLatch;
pub use multi::MultiLockHandle;
pub use redlock::{RedlockConfig, RedlockHandle, RedlockManager};
pub use reentrant::ReentrantLockHandle;
pub use rwlock::{RwReadHandle, RwWriteHandle};
pub use semaphore::{RedisSemaphore, SemaphorePermit};
pub use single::{RedisLockHandle, RedisLockManager};
