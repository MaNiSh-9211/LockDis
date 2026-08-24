//! INV-1 · The Black Box Recorder.
//!
//! A tamper-evident, hash-chained flight recorder for lock operations.
//! Every frame commits to the previous one (`frame_hash = H(prev || body)`),
//! so any retroactive edit breaks the chain and [`BlackBox::verify_chain`]
//! catches it. On anomalies callers dump the tape: you replay the exact
//! causal sequence instead of reconstructing it from logs.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// One recorded operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    /// Monotonic sequence number (0-based).
    pub seq: u64,
    pub key: String,
    pub op: &'static str,
    pub ok: bool,
    /// Fence token in effect (0 when not applicable).
    pub fence: u64,
    /// Hash of this frame's body chained with the previous frame's hash.
    pub chain: u64,
}

fn mix(mut h: u64, byte: u8) -> u64 {
    h ^= byte as u64;
    h = h.wrapping_mul(0x100_0000_01b3); // FNV prime
    h
}

fn chain_of(prev: u64, key: &str, op: &str, ok: bool, fence: u64) -> u64 {
    let mut h = prev ^ 0xcbf2_9ce4_8422_2325; // offset basis
    for b in key.bytes() {
        h = mix(h, b);
    }
    h = mix(h, 0);
    for b in op.bytes() {
        h = mix(h, b);
    }
    h = mix(h, 0);
    h = mix(h, u8::from(ok));
    for shift in [56, 48, 40, 32, 24, 16, 8, 0] {
        h = mix(h, (fence >> shift) as u8);
    }
    h
}

/// Ring-buffered, hash-chained operation recorder. Clone-cheap.
#[derive(Clone)]
pub struct BlackBox {
    inner: Arc<Mutex<State>>,
}

struct State {
    buf: VecDeque<Frame>,
    cap: usize,
    last_chain: u64,
    next_seq: u64,
}

impl Default for BlackBox {
    fn default() -> Self {
        Self::with_capacity(1024)
    }
}

impl BlackBox {
    /// Creates a recorder keeping the most recent `cap` frames.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(State {
                buf: VecDeque::with_capacity(cap),
                cap,
                last_chain: 0,
                next_seq: 0,
            })),
        }
    }

    /// Records one completed operation.
    pub fn record(&self, key: &str, op: &'static str, ok: bool, fence: u64) {
        let mut st = self.inner.lock().expect("blackbox");
        let seq = st.next_seq;
        st.next_seq += 1;
        let chain = chain_of(st.last_chain, key, op, ok, fence);
        st.last_chain = chain;
        if st.buf.len() == st.cap {
            st.buf.pop_front();
        }
        st.buf.push_back(Frame {
            seq,
            key: key.to_owned(),
            op,
            ok,
            fence,
            chain,
        });
    }

    /// Snapshot of retained frames in insertion order.
    pub fn dump(&self) -> Vec<Frame> {
        self.inner
            .lock()
            .expect("blackbox")
            .buf
            .iter()
            .cloned()
            .collect()
    }

    /// Verifies hash-chain linkage across retained frames. The first
    /// retained frame is trusted as the anchor (its predecessor may have
    /// been legitimately evicted); every subsequent frame must commit to
    /// its predecessor's stored chain.
    pub fn verify_chain(&self) -> Result<(), u64> {
        let st = self.inner.lock().expect("blackbox");
        let mut prev = match st.buf.front() {
            None => return Ok(()),
            Some(first) => first.chain,
        };
        for f in st.buf.iter().skip(1) {
            if chain_of(prev, &f.key, f.op, f.ok, f.fence) != f.chain {
                return Err(f.seq);
            }
            prev = f.chain;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_verifies_on_clean_tape() {
        let bb = BlackBox::default();
        bb.record("k", "grant", true, 1);
        bb.record("k", "extend", true, 1);
        bb.record("k", "release", true, 1);
        assert!(bb.verify_chain().is_ok());
    }

    #[test]
    fn tape_eviction_keeps_verification_sound() {
        let bb = BlackBox::with_capacity(4);
        for i in 0..50 {
            bb.record("k", "grant", true, i);
        }
        assert_eq!(bb.dump().len(), 4);
        assert!(bb.verify_chain().is_ok());
    }

    #[test]
    fn frames_carry_causal_context() {
        let bb = BlackBox::default();
        bb.record("orders/7", "grant", true, 9);
        bb.record("orders/7", "lost", false, 9);
        let tape = bb.dump();
        assert_eq!(tape[0].op, "grant");
        assert_eq!(tape[1].op, "lost");
        assert!(!tape[1].ok);
    }
}
