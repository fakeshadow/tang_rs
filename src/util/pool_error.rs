use core::fmt;

use std::error;

/// Error which occurs when popping from an empty queue.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum PopError {
    /// The queue is empty but not closed.
    Empty,

    /// A new `T` must be spawned now.
    ///
    /// *. Whoever get hold of SpawnNow error must take responsibility of spawn new `T` and call
    /// `PoolInner::push_new`(Or `PoolInner::dec_pending` if it fails at spawning)
    SpawnNow,
}

impl PopError {
    pub(crate) fn is_spawn_now(self) -> bool {
        self == PopError::SpawnNow
    }
}

impl error::Error for PopError {}

impl fmt::Debug for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PopError::Empty => write!(f, "Empty"),
            PopError::SpawnNow => write!(f, "SpawnNow"),
        }
    }
}

impl fmt::Display for PopError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PopError::Empty => write!(f, "Empty"),
            PopError::SpawnNow => write!(f, "SpawnNow"),
        }
    }
}

/// Error which occurs when pushing into a full or closed queue.
#[derive(Clone, Copy, Eq, PartialEq)]
#[allow(dead_code)]
pub(crate) enum PushError {
    /// The queue is full but not closed.
    Full,
}

impl error::Error for PushError {}

impl fmt::Debug for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Full => f.debug_tuple("Full").finish(),
        }
    }
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Full => write!(f, "Full"),
        }
    }
}
