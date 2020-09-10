pub(crate) enum PopError {
    /// The queue is empty.
    Empty,

    /// A new `T` must be spawned now.
    ///
    /// *. Whoever get hold of SpawnNow error must take responsibility of spawn new `T`
    SpawnNow,
}

impl PopError {
    pub(crate) fn is_spawn_now(&self) -> bool {
        matches!(self, PopError::SpawnNow)
    }
}

#[allow(dead_code)]
pub(crate) enum PushError {
    /// The queue is full but not closed.
    Full,
}
