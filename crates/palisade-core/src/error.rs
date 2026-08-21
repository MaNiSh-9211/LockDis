use thiserror::Error;

/// Convenience alias for results produced by Palisade APIs.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors surfaced by Palisade locking operations.
#[derive(Debug, Error)]
pub enum Error {
    /// The caller gave up waiting before the lock became available.
    #[error("timed out acquiring lock `{key}`")]
    Timeout {
        /// Key of the lock that could not be acquired in time.
        key: String,
    },

    /// An immediate try-lock found the lock held by someone else.
    #[error("lock `{key}` is already held")]
    Held {
        /// Key of the contended lock.
        key: String,
    },

    /// The lease expired or was revoked mid-hold. The critical section must
    /// abort; downstream writes should be rejected using the fencing token.
    #[error(
        "lock `{key}` was lost while held: the lease expired or was revoked; \
         writes after this point must be rejected via fencing token {fence}"
    )]
    Lost {
        /// Key of the lost lock.
        key: String,
        /// Last fencing token observed by the holder.
        fence: u64,
    },

    /// Builder options are contradictory or out of range.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// The storage backend failed (I/O, serialization, script error, ...).
    #[error("backend error: {0}")]
    Backend(String),
}
