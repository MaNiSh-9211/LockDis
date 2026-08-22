//! Embedded Lua scripts (see PLAN.md §4 and the ADRs for the safety
//! arguments behind each one).

pub(crate) const ACQUIRE: &str = include_str!("scripts/acquire.lua");
pub(crate) const RELEASE: &str = include_str!("scripts/release.lua");
pub(crate) const EXTEND: &str = include_str!("scripts/extend.lua");

pub(crate) const REENTRANT_ACQUIRE: &str = include_str!("scripts/reentrant_acquire.lua");
pub(crate) const REENTRANT_RELEASE: &str = include_str!("scripts/reentrant_release.lua");
pub(crate) const REENTRANT_RELEASE_ALL: &str = include_str!("scripts/reentrant_release_all.lua");
pub(crate) const REENTRANT_EXTEND: &str = include_str!("scripts/reentrant_extend.lua");

pub(crate) const RW_READ_ACQUIRE: &str = include_str!("scripts/rw_read_acquire.lua");
pub(crate) const RW_READ_RELEASE: &str = include_str!("scripts/rw_read_release.lua");
pub(crate) const RW_WRITE_ACQUIRE: &str = include_str!("scripts/rw_write_acquire.lua");
pub(crate) const RW_WRITE_RELEASE: &str = include_str!("scripts/rw_write_release.lua");
pub(crate) const RW_EXTEND: &str = include_str!("scripts/rw_extend.lua");

pub(crate) const SEMAPHORE_ACQUIRE: &str = include_str!("scripts/semaphore_acquire.lua");
pub(crate) const SEMAPHORE_RELEASE: &str = include_str!("scripts/semaphore_release.lua");
pub(crate) const SEMAPHORE_EXTEND: &str = include_str!("scripts/semaphore_extend.lua");

pub(crate) const FAIR_ACQUIRE: &str = include_str!("scripts/fair_acquire.lua");
pub(crate) const FAIR_RELEASE: &str = include_str!("scripts/fair_release.lua");
