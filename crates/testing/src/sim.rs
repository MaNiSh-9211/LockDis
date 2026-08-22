//! Deterministic simulation core: a virtual-time lock store plus a seeded
//! scheduler driving worker state machines, with fault injection.
//!
//! Why bespoke instead of madsim (ADR 0025): we are validating the
//! *algorithm* (CAS + TTL + fencing), not the network. A single-threaded
//! event loop over virtual time gives bit-exact reproducibility from a seed
//! with zero dependencies, and every dangerous interleaving - pause past
//! TTL, release-after-expiry, grant-while-held - is reachable by direct
//! injection. The real-network layer is validated separately by the chaos
//! suite.

use palisade_core::FencingToken;

/// One observed occurrence in a simulated run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    /// Acquire succeeded.
    Grant,
    /// Acquire denied (held elsewhere).
    Deny,
    /// Release succeeded while still valid.
    ReleaseOk,
    /// Release found the lease already gone.
    ReleaseLost,
    /// Fenced write accepted by the resource.
    WriteAccepted,
    /// Fenced write rejected by the resource.
    WriteRejected,
}

/// A recorded history entry: `(virtual time, worker, kind, fence)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event {
    /// Virtual time in milliseconds.
    pub t_ms: u64,
    /// Worker index.
    pub worker: usize,
    pub kind: EventKind,
    /// Fence token in effect for this event (0 where not applicable).
    pub fence: u64,
}

/// Knobs for one simulated run. Bug-injection flags exist to prove the
/// checker catches violations - a validator that has never seen a failure
/// proves nothing.
#[derive(Clone, Debug)]
pub struct Scenario {
    pub seed: u64,
    pub workers: usize,
    /// Decision points per worker before it stops.
    pub steps_per_worker: u32,
    pub ttl_ms: u64,
    /// Percentage chance that a hold turns into a pause lasting past the
    /// lease expiry (models GC stop / SIGSTOP).
    pub pause_probability_pct: u32,
    /// BUG INJECTION: store grants even when the key is live-held.
    pub broken_cas: bool,
    /// BUG INJECTION: resource accepts non-monotonic / stale fences.
    pub broken_fencing: bool,
}

impl Scenario {
    /// Clean scenario: correct algorithms, aggressive pause faults.
    pub fn clean(seed: u64) -> Self {
        Self {
            seed,
            workers: 6,
            steps_per_worker: 40,
            ttl_ms: 100,
            pause_probability_pct: 35,
            broken_cas: false,
            broken_fencing: false,
        }
    }
}

/// Tiny xorshift64 - deterministic on every platform, no dependencies.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next_u64() % n }
    }

    fn pct(&mut self, percent: u32) -> bool {
        self.below(100) < u64::from(percent)
    }
}

#[derive(Clone, Copy)]
struct Entry {
    holder: usize,
    expires_ms: u64,
    fence: u64,
}

/// Single-key mirror of the Lua semantics: CAS grant with TTL, ownership-
/// checked release, monotonic fence counter, and a fence-checking
/// "protected resource".
struct SimStore {
    now_ms: u64,
    entry: Option<Entry>,
    fence_counter: u64,
    last_accepted_fence: u64,
    broken_cas: bool,
    broken_fencing: bool,
}

impl SimStore {
    fn cas_grant(&mut self, worker: usize, ttl_ms: u64) -> Option<u64> {
        if let Some(e) = self.entry {
            if e.expires_ms > self.now_ms && !self.broken_cas {
                return None;
            }
        }
        self.fence_counter += 1;
        self.entry = Some(Entry {
            holder: worker,
            expires_ms: self.now_ms + ttl_ms,
            fence: self.fence_counter,
        });
        Some(self.fence_counter)
    }

    /// Ownership-checked release; `true` = released, `false` = lost.
    /// Mirrors Redis: an expired key is already gone, so releasing it fails.
    fn release(&mut self, worker: usize, fence: u64) -> bool {
        match self.entry {
            Some(e) if e.holder == worker && e.fence == fence && e.expires_ms > self.now_ms => {
                self.entry = None;
                true
            }
            _ => false,
        }
    }

    /// Is `worker`'s lease with `fence` currently valid?
    fn holds_validly(&self, worker: usize, fence: u64) -> bool {
        matches!(self.entry,
            Some(e) if e.holder == worker && e.fence == fence && e.expires_ms > self.now_ms)
    }

    /// The protected resource: accepts writes only from a validly-holding
    /// worker whose fence strictly supersedes everything accepted so far.
    fn fenced_write(&mut self, worker: usize, fence: u64) -> bool {
        if self.broken_fencing {
            return true;
        }
        if !self.holds_validly(worker, fence) {
            return false;
        }
        if FencingToken::new(fence).supersedes(FencingToken::new(self.last_accepted_fence)) {
            self.last_accepted_fence = fence;
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Copy)]
enum State {
    /// Ready to make its next decision; `steps_left` counts down monotonically.
    Idle {
        steps_left: u32,
    },
    Holding {
        fence: u64,
        release_at: u64,
        will_pause: bool,
        budget: u32,
    },
    Paused {
        fence: u64,
        budget: u32,
    },
    Done,
}

fn run(sc: &Scenario) -> Vec<Event> {
    assert!(sc.workers >= 2, "contention requires at least two workers");
    let mut rng = Rng::new(sc.seed);
    let mut store = SimStore {
        now_ms: 0,
        entry: None,
        fence_counter: 0,
        last_accepted_fence: 0,
        broken_cas: sc.broken_cas,
        broken_fencing: sc.broken_fencing,
    };
    let total_steps = sc.steps_per_worker;
    let mut states = vec![
        State::Idle {
            steps_left: total_steps
        };
        sc.workers
    ];
    let mut wake_at = vec![0u64; sc.workers];
    let mut events = Vec::new();
    let mut ev = |t: u64, w: usize, kind: EventKind, fence: u64| {
        events.push(Event {
            t_ms: t,
            worker: w,
            kind,
            fence,
        })
    };

    while let Some(w) = (0..sc.workers)
        .filter(|&w| !matches!(states[w], State::Done))
        .min_by_key(|&w| (wake_at[w], w))
    {
        store.now_ms = store.now_ms.max(wake_at[w]);

        states[w] = match states[w] {
            State::Idle { steps_left } => {
                if steps_left == 0 {
                    wake_at[w] = u64::MAX;
                    State::Done
                } else if rng.below(100) < 70 {
                    match store.cas_grant(w, sc.ttl_ms) {
                        Some(fence) => {
                            ev(store.now_ms, w, EventKind::Grant, fence);
                            let will_pause = rng.pct(sc.pause_probability_pct);
                            let hold = 5 + rng.below(sc.ttl_ms / 2);
                            let release_at = store.now_ms + hold;
                            // A pausing worker wakes mid-hold and freezes there.
                            wake_at[w] = if will_pause {
                                store.now_ms + hold / 2
                            } else {
                                release_at
                            };
                            State::Holding {
                                fence,
                                release_at,
                                will_pause,
                                budget: steps_left - 1,
                            }
                        }
                        None => {
                            ev(store.now_ms, w, EventKind::Deny, 0);
                            wake_at[w] += 1 + rng.below(3);
                            State::Idle {
                                steps_left: steps_left - 1,
                            }
                        }
                    }
                } else {
                    wake_at[w] += 1;
                    State::Idle {
                        steps_left: steps_left - 1,
                    }
                }
            }
            State::Holding {
                fence,
                release_at,
                will_pause,
                budget,
            } => {
                if will_pause && store.now_ms < release_at {
                    // SIGSTOP/GC model: freeze mid-hold, wake past lease death.
                    wake_at[w] = release_at + sc.ttl_ms + 10;
                    State::Paused { fence, budget }
                } else if store.now_ms >= release_at {
                    // Fenced write happens while still (nominally) holding...
                    let write_ok = store.fenced_write(w, fence);
                    ev(
                        store.now_ms,
                        w,
                        if write_ok {
                            EventKind::WriteAccepted
                        } else {
                            EventKind::WriteRejected
                        },
                        fence,
                    );
                    // ...then the release tells us whether the lease survived.
                    let released = store.release(w, fence);
                    ev(
                        store.now_ms,
                        w,
                        if released {
                            EventKind::ReleaseOk
                        } else {
                            EventKind::ReleaseLost
                        },
                        fence,
                    );
                    wake_at[w] = store.now_ms + rng.below(4);
                    State::Idle { steps_left: budget }
                } else {
                    wake_at[w] = release_at;
                    State::Holding {
                        fence,
                        release_at,
                        will_pause,
                        budget,
                    }
                }
            }
            State::Paused { fence, budget } => {
                // Woke up after the lease died: the write must be rejected...
                let write_ok = store.fenced_write(w, fence);
                ev(
                    store.now_ms,
                    w,
                    if write_ok {
                        EventKind::WriteAccepted
                    } else {
                        EventKind::WriteRejected
                    },
                    fence,
                );
                // -and the release must report loss, never delete a successor's lock.
                let released = store.release(w, fence);
                debug_assert!(!released, "paused-past-ttl release must report loss");
                ev(store.now_ms, w, EventKind::ReleaseLost, fence);
                wake_at[w] = store.now_ms + 1;
                State::Idle { steps_left: budget }
            }
            State::Done => unreachable!("done workers are filtered out"),
        };
    }
    events.sort_by_key(|e| e.t_ms);
    events
}

/// Runs `scenario` to completion and returns the recorded history.
pub fn simulate(scenario: &Scenario) -> Vec<Event> {
    run(scenario)
}
