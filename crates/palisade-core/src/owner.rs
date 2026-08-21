use std::fmt;

/// Identifies the current holder of a lock.
///
/// Backends store this value as the lock payload so that release/extend
/// scripts can verify ownership before mutating state. UUIDv7 gives us
/// time-ordered, collision-free identifiers without coordination.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct OwnerId(uuid::Uuid);

impl OwnerId {
    /// Generates a fresh owner id (UUIDv7, time-ordered).
    pub fn generate() -> Self {
        Self(uuid::Uuid::now_v7())
    }

    /// Wraps an existing UUID (e.g. reconstructed from a serialized handle).
    pub fn from_uuid(id: uuid::Uuid) -> Self {
        Self(id)
    }

    pub fn as_uuid(&self) -> uuid::Uuid {
        self.0
    }
}

impl fmt::Display for OwnerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Debug for OwnerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OwnerId({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generated_ids_are_unique() {
        let a = OwnerId::generate();
        let b = OwnerId::generate();
        assert_ne!(a, b);
    }

    #[test]
    fn round_trips_through_uuid() {
        let a = OwnerId::generate();
        let b = OwnerId::from_uuid(a.as_uuid());
        assert_eq!(a, b);
    }
}
