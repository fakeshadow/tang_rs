use core::cell::Cell;
use core::sync::atomic::spin_loop_hint;

use std::thread::yield_now;

/// A simple backoff (`crossbeam-util::backoff::BackOff` with yield limit disabled.)
pub(crate) struct Backoff {
    cycle: Cell<u8>,
}

const SPIN_LIMIT: u8 = 6;

impl Backoff {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            cycle: Cell::new(0),
        }
    }

    #[inline]
    pub(crate) fn snooze(&self) {
        if self.cycle.get() <= SPIN_LIMIT {
            for _ in 0..1 << self.cycle.get() {
                spin_loop_hint();
            }
        } else {
            return yield_now();
        }

        self.cycle.set(self.cycle.get() + 1);
    }
}
