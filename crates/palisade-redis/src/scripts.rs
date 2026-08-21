//! Embedded Lua scripts (see PLAN.md §4.1 for the safety arguments
//! behind each one).

pub(crate) const ACQUIRE: &str = include_str!("scripts/acquire.lua");
pub(crate) const RELEASE: &str = include_str!("scripts/release.lua");
pub(crate) const EXTEND: &str = include_str!("scripts/extend.lua");
