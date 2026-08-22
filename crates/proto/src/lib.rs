//! Palisade wire contract.
//!
//! Generated types live under `palisade::v1` (from `proto/palisade.v1.proto`).
//! The `.proto` file is the public, versioned contract: backward-compatible
//! evolution only (see ADR 0008).

pub mod palisade {
    include!(concat!(env!("OUT_DIR"), "/palisade.v1.rs"));
}

pub use palisade::*;
