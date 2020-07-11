/// From [concurrent-queue](https://crates.io/crate/concurrent-queue)
///
/// Changes:
/// - remove `mark_bit` as we want to close the queue from outside.
/// - add `active_pending` field for tracking the active and pending item count of queue
///   (Both inside and outside of queue for a given moment)
/// - add `shift` field for comparing `active_pending` with `cap`.
/// - `one_lap` serves as the `mark_bit` of both `head`/`tail` and `active_pending`.
use core::cell::UnsafeCell;
use core::fmt;
use core::marker::PhantomData;
use core::mem::{self, MaybeUninit};
use core::sync::atomic::{self, AtomicUsize, Ordering};
use std::{error, thread};

use cache_padded::CachePadded;

/// A slot in a queue.
struct Slot<T> {
    /// The current stamp.
    stamp: AtomicUsize,

    /// The value in this slot.
    value: UnsafeCell<MaybeUninit<T>>,
}

/// A bounded queue.
pub struct LockFreePool<T> {
    /// The current count of active T and pending T of this queue.
    ///
    /// They may or may not in the queue currently.
    ///
    /// We pack the two counts into one usize with `{ active: 0, pending: 0 }`
    active_pending: CachePadded<AtomicUsize>,

    /// The head of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer, a mark bit, and a lap, but
    /// packed into a single `usize`. The lower bits represent the index, while the upper bits
    /// represent the lap. The mark bit in the head is always zero.
    ///
    /// Values are popped from the head of the queue.
    head: CachePadded<AtomicUsize>,

    /// The tail of the queue.
    ///
    /// This value is a "stamp" consisting of an index into the buffer, a mark bit, and a lap, but
    /// packed into a single `usize`. The lower bits represent the index, while the upper bits
    /// represent the lap. The mark bit indicates that the queue is closed.
    ///
    /// Values are pushed into the tail of the queue.
    tail: CachePadded<AtomicUsize>,

    /// The buffer holding slots.
    buffer: *mut Slot<T>,

    /// The queue capacity.
    cap: usize,

    /// A stamp with the value of `{ lap: 1, index: 0 }`.
    one_lap: usize,

    /// We use this to shift the active_pending into/from an actual active count.
    shift: usize,

    /// Indicates that dropping an `Bounded<T>` may drop values of type `T`.
    _marker: PhantomData<T>,
}

impl<T> LockFreePool<T> {
    /// Creates a new bounded queue.
    pub fn new(cap: usize) -> LockFreePool<T> {
        assert!(cap > 0, "capacity must be positive");

        // Head is initialized to `{ lap: 0, index: 0 }`.
        let head = 0;
        // Tail is initialized to `{ lap: 0, index: 0 }`.
        let tail = 0;

        // Allocate a buffer of `cap` slots initialized with stamps.
        let buffer = {
            let mut v: Vec<Slot<T>> = (0..cap)
                .map(|i| {
                    // Set the stamp to `{ lap: 0, index: i }`.
                    Slot {
                        stamp: AtomicUsize::new(i),
                        value: UnsafeCell::new(MaybeUninit::uninit()),
                    }
                })
                .collect();

            let ptr = v.as_mut_ptr();
            mem::forget(v);
            ptr
        };

        // Compute constants `one_lap` and `shift`.
        let one_lap = (cap + 1).next_power_of_two();
        let shift = (one_lap - 1).count_ones() as usize;

        LockFreePool {
            active_pending: CachePadded::new(AtomicUsize::new(0)),
            buffer,
            cap,
            one_lap,
            shift,
            head: CachePadded::new(AtomicUsize::new(head)),
            tail: CachePadded::new(AtomicUsize::new(tail)),
            _marker: PhantomData,
        }
    }

    /// Attempts to push a new item into the queue.
    pub fn push_new(&self, value: T) -> Result<(), PushError> {
        match self.push_back(value) {
            Ok(()) => {
                // push success we increment active count and decrement pending count both by 1.
                let mut active_pending = self.active_pending.load(Ordering::Relaxed);
                loop {
                    let active_pending_new = active_pending + self.one_lap - 1;

                    match self.active_pending.compare_exchange_weak(
                        active_pending,
                        active_pending_new,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(acp) => {
                            active_pending = acp;
                        }
                    }
                }

                Ok(())
            }
            Err(e) => {
                // push failed for some reason and we just decrement pending count.
                self.active_pending.fetch_sub(1, Ordering::SeqCst);
                Err(e)
            }
        }
    }

    /// *. NOTE: Whoever take the ownership of `PopError::SpawnNow` is responsible to call
    /// `Bounded::notify_dec_pending` if it failed to spawn a new item for queue.
    pub fn notify_dec_pending(&self) {
        self.active_pending.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn notify_dec_active(&self) {
        self.active_pending
            .fetch_sub(self.one_lap, Ordering::SeqCst);
    }

    /// Attempts to push a returning item into the queue.
    pub fn push_back(&self, value: T) -> Result<(), PushError> {
        let mut tail = self.tail.load(Ordering::Relaxed);

        loop {
            // Deconstruct the tail.
            let index = tail & (self.one_lap - 1);
            let lap = tail & !(self.one_lap - 1);

            // Inspect the corresponding slot.
            let slot = unsafe { &*self.buffer.add(index) };
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
    ///
    /// When we get an error for popping we would check the active_pending count and return
    /// `PopError::SpawnNow` to notify the caller it's time to spawn new item for the queue.
    ///
    /// *. NOTE: Whoever take the ownership of `PopError::SpawnNow` is responsible for spawn the
    /// new item and call `Bounded::push_new()` to insert the new item.
    /// (or call `Bounded::notify_dec_pending` if it failed to do so.)
    pub fn pop(&self) -> Result<T, PopError> {
        self._pop().map_err(|e| {
            // read the active pending
            let mut active_pending = self.active_pending.load(Ordering::Relaxed);
            loop {
                let pending = active_pending & (self.one_lap - 1);
                // ToDo: find a better way to check the count without shift.
                let active = (active_pending & !(self.one_lap - 1)) >> self.shift;

                // if we are at the cap then just break and return empty error.
                if active + pending == self.cap {
                    break;
                } else {
                    // otherwise we increment pending count and try to write.
                    let new_active_pending = active_pending + 1;

                    match self.active_pending.compare_exchange_weak(
                        active_pending,
                        new_active_pending,
                        Ordering::SeqCst,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => {
                            // write success we notify the caller it's time to spawn
                            return PopError::SpawnNow;
                        }
                        Err(acp) => {
                            active_pending = acp;
                        }
                    }
                }
            }
            e
        })
    }

    /// Attempts to pop an item from the queue.
    fn _pop(&self) -> Result<T, PopError> {
        let mut head = self.head.load(Ordering::Relaxed);

        loop {
            // Deconstruct the head.
            let index = head & (self.one_lap - 1);
            let lap = head & !(self.one_lap - 1);

            // Inspect the corresponding slot.
            let slot = unsafe { &*self.buffer.add(index) };
            let stamp = slot.stamp.load(Ordering::Acquire);

            // If the the stamp is ahead of the head by 1, we may attempt to pop.
            if head + 1 == stamp {
                let new = if index + 1 < self.cap {
                    // Same lap, incremented index.
                    // Set to `{ lap: lap, mark: 0, index: index + 1 }`.
                    head + 1
                } else {
                    // One lap forward, index wraps around to zero.
                    // Set to `{ lap: lap.wrapping_add(1), mark: 0, index: 0 }`.
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
    pub fn len(&self) -> usize {
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
                    self.cap - hix + tix
                } else if tail == head {
                    0
                } else {
                    self.cap
                };
            }
        }
    }

    /// Returns `true` if the queue is full.
    pub fn state(&self) -> (usize, usize, usize) {
        let active_pending = self.active_pending.load(Ordering::SeqCst);
        let len = self.len();
        let pending = active_pending & (self.one_lap - 1);
        let active = (active_pending & !(self.one_lap - 1)) >> self.shift;

        (len, active, pending)
    }
}

impl<T> Drop for LockFreePool<T> {
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
            unsafe {
                let slot = &*self.buffer.add(index);
                let value = slot.value.get().read().assume_init();
                drop(value);
            }
        }

        // Finally, deallocate the buffer, but don't run any destructors.
        unsafe {
            Vec::from_raw_parts(self.buffer, 0, self.cap);
        }
    }
}

/// Error which occurs when popping from an empty queue.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum PopError {
    /// The queue is empty but not closed.
    Empty,

    /// A new T must be spawned now.
    ///
    /// *. Whoever get hold of SpawnNow error must take responsibility of spawn new T and call
    /// `Bounded::push_spawn`(Even when the spawn failed)
    SpawnNow,
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
pub enum PushError {
    /// The queue is full but not closed.
    Full,

    /// The queue is closed.
    Closed,
}

impl error::Error for PushError {}

impl fmt::Debug for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Full => f.debug_tuple("Full").finish(),
            PushError::Closed => f.debug_tuple("Closed").finish(),
        }
    }
}

impl fmt::Display for PushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PushError::Full => write!(f, "Full"),
            PushError::Closed => write!(f, "Closed"),
        }
    }
}
