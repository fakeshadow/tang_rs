/// This code is a mostly a copy/paste from [concurrent-queue](https://crates.io/crate/concurrent-queue)
///
/// Changes:
/// - remove `mark_bit` as we want to close/replace the queue from outside.
/// - add `active_pending` field for tracking the active and pending item count of queue
///   (Both inside and outside of queue for a given moment)
/// - add `shift` field for comparing `active_pending` with `cap`.
/// - `one_lap` serves as the bitwise operator of both `head`/`tail` and `active_pending`.
use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::sync::atomic::{self, AtomicUsize, Ordering};

use std::thread;

use cache_padded::CachePadded;

use crate::util::pool_error::{PopError, PushError};

/// A slot in a queue.
struct Slot<T> {
    /// The current stamp.
    stamp: CachePadded<AtomicUsize>,

    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded queue.
pub(crate) struct AtomicPool<T> {
    /// The current count of active T.
    ///
    /// They may or may not in the queue currently.
    active: CachePadded<AtomicUsize>,

    /// The head of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but
    /// packed into a single `usize`. The lower bits represent the index, while the upper bits
    /// represent the lap.
    ///
    /// Values are popped from the head of the queue.
    head: CachePadded<AtomicUsize>,

    /// The tail of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer and a lap, but
    /// packed into a single `usize`. The lower bits represent the index, while the upper bits
    /// represent the lap.
    ///
    /// Values are pushed into the tail of the queue.
    tail: CachePadded<AtomicUsize>,

    /// The buffer holding slots.
    buffer: Box<[Slot<T>]>,

    /// The queue capacity.
    cap: usize,

    /// A stamp with the value of `{ lap: 1, index: 0 }`.
    one_lap: usize,
}

impl<T> AtomicPool<T> {
    /// Creates a new bounded queue.
    pub(crate) fn new(cap: usize) -> AtomicPool<T> {
        assert!(cap > 0, "capacity must be positive");

        // Head is initialized to `{ lap: 0, index: 0 }`.
        let head = 0;
        // Tail is initialized to `{ lap: 0, index: 0 }`.
        let tail = 0;

        // Allocate a buffer of `cap` slots initialized with stamps.
        let mut buffer = Vec::with_capacity(cap);
        for i in 0..cap {
            // Set the stamp to `{ lap: 0, index: i }`.
            buffer.push(Slot {
                stamp: CachePadded::new(AtomicUsize::new(i)),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        // Compute constants `one_lap` and `shift`.
        let one_lap = (cap + 1).next_power_of_two();

        AtomicPool {
            active: CachePadded::new(AtomicUsize::new(0)),
            head: CachePadded::new(AtomicUsize::new(head)),
            tail: CachePadded::new(AtomicUsize::new(tail)),
            buffer: buffer.into(),
            cap,
            one_lap,
        }
    }

    pub(crate) fn inc_active(&self, count: usize) {
        self.active.fetch_add(count, Ordering::SeqCst);
    }

    pub(crate) fn dec_active(&self, count: usize) -> usize {
        let active = self.active.fetch_sub(count, Ordering::SeqCst);
        active - count
    }

    /// Attempts to push a returning item into the queue.
    pub(crate) fn push_back(&self, value: T) {
        // We assume the push will never fail.
        let _ = self.push(value);
    }

    /// Attempts to pop an item from the queue.
    ///
    /// When we get an error for popping we would check the active_pending count and return
    /// `PopError::SpawnNow` to notify the caller it's time to spawn new item for the queue.
    ///
    /// *. NOTE: Whoever take the ownership of `PopError::SpawnNow` is responsible for spawn the
    /// new item and call `PoolInner::push_new` to insert the new item.
    /// (or call `PoolInner::dec_pending` if it failed to do so.)
    pub(crate) fn pop(&self) -> Result<T, PopError> {
        self._pop().map_err(|e| self._inc_active(e))
    }

    fn _inc_active(&self, e: PopError) -> PopError {
        let mut active = self.active.load(Ordering::Relaxed);
        loop {
            // if we are at the cap then just break and return empty error.
            if active == self.cap {
                break;
            } else {
                // otherwise we increment pending count and try to write.
                match self.active.compare_exchange_weak(
                    active,
                    active + 1,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // write success we notify the caller it's time to spawn
                        return PopError::SpawnNow;
                    }
                    Err(acp) => {
                        active = acp;
                    }
                }
            }
        }
        e
    }

    /// Attempts to push an item into the queue.
    fn push(&self, value: T) -> Result<(), PushError> {
        let mut tail = self.tail.load(Ordering::Relaxed);

        loop {
            // Deconstruct the tail.
            let index = tail & (self.one_lap - 1);
            let lap = tail & !(self.one_lap - 1);

            // Inspect the corresponding slot.
            let slot = &self.buffer[index];
            let stamp = slot.stamp.load(Ordering::Acquire);

            // If the tail and the stamp match, we may attempt to push.
            if tail == stamp {
                let new_tail = if index + 1 < self.cap {
                    // Same lap, incremented index.
                    // Set to `{ lap: lap, index: index + 1 }`.
                    tail + 1
                } else {
                    // One lap forward, index wraps around to zero.
                    // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
                    lap.wrapping_add(self.one_lap)
                };

                // Try moving the tail.
                match self.tail.compare_exchange_weak(
                    tail,
                    new_tail,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // Write the value into the slot and update the stamp.
                        unsafe {
                            slot.value.get().write(MaybeUninit::new(value));
                        }
                        slot.stamp.store(tail + 1, Ordering::Release);
                        return Ok(());
                    }
                    Err(t) => {
                        tail = t;
                    }
                }
            } else if stamp.wrapping_add(self.one_lap) == tail + 1 {
                atomic::fence(Ordering::SeqCst);
                let head = self.head.load(Ordering::Relaxed);

                // If the head lags one lap behind the tail as well...
                if head.wrapping_add(self.one_lap) == tail {
                    // ...then the queue is full.
                    return Err(PushError::Full);
                }

                tail = self.tail.load(Ordering::Relaxed);
            } else {
                // Yield because we need to wait for the stamp to get updated.
                thread::yield_now();
                tail = self.tail.load(Ordering::Relaxed);
            }
        }
    }

    /// Attempts to pop an item from the queue.
    fn _pop(&self) -> Result<T, PopError> {
        let mut head = self.head.load(Ordering::Relaxed);

        loop {
            // Deconstruct the head.
            let index = head & (self.one_lap - 1);
            let lap = head & !(self.one_lap - 1);

            // Inspect the corresponding slot.
            let slot = &self.buffer[index];
            let stamp = slot.stamp.load(Ordering::Acquire);

            // If the the stamp is ahead of the head by 1, we may attempt to pop.
            if head + 1 == stamp {
                let new = if index + 1 < self.cap {
                    // Same lap, incremented index.
                    // Set to `{ lap: lap, index: index + 1 }`.
                    head + 1
                } else {
                    // One lap forward, index wraps around to zero.
                    // Set to `{ lap: lap.wrapping_add(1), index: 0 }`.
                    lap.wrapping_add(self.one_lap)
                };

                // Try moving the head.
                match self.head.compare_exchange_weak(
                    head,
                    new,
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // Read the value from the slot and update the stamp.
                        let value = unsafe { slot.value.get().read().assume_init() };
                        slot.stamp
                            .store(head.wrapping_add(self.one_lap), Ordering::Release);
                        return Ok(value);
                    }
                    Err(h) => {
                        head = h;
                    }
                }
            } else if stamp == head {
                atomic::fence(Ordering::SeqCst);

                // If the tail equals the head, that means the queue is empty.
                if self.tail.load(Ordering::Relaxed) == head {
                    return Err(PopError::Empty);
                }

                head = self.head.load(Ordering::Relaxed);
            } else {
                // Yield because we need to wait for the stamp to get updated.
                thread::yield_now();
                head = self.head.load(Ordering::Relaxed);
            }
        }
    }

    /// Returns the number of items in the queue.
    pub(crate) fn len(&self) -> usize {
        loop {
            // Load the tail, then load the head.
            let tail = self.tail.load(Ordering::SeqCst);
            let head = self.head.load(Ordering::SeqCst);

            // If the tail didn't change, we've got consistent values to work with.
            if self.tail.load(Ordering::SeqCst) == tail {
                let hix = head & (self.one_lap - 1);
                let tix = tail & (self.one_lap - 1);

                return if hix < tix {
                    tix - hix
                } else if hix > tix {
                    self.buffer.len() - hix + tix
                } else if tail == head {
                    0
                } else {
                    self.cap
                };
            }
        }
    }

    /// Returns items in queue, active and pending count in tuple
    pub(crate) fn state(&self) -> (usize, usize) {
        let active = self.active.load(Ordering::Relaxed);
        let len = self.len();

        (len, active)
    }
}

impl<T> Drop for AtomicPool<T> {
    fn drop(&mut self) {
        // Get the index of the head.
        let hix = self.head.load(Ordering::Relaxed) & (self.one_lap - 1);

        // Loop over all slots that hold a value and drop them.
        for i in 0..self.len() {
            // Compute the index of the next slot holding a value.
            let index = if hix + i < self.cap {
                hix + i
            } else {
                hix + i - self.cap
            };

            // Drop the value in the slot.
            let slot = &self.buffer[index];
            unsafe {
                let value = slot.value.get().read().assume_init();
                drop(value);
            }
        }
    }
}
