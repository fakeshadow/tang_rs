use core::marker::PhantomData;
use core::mem::replace;
use core::num::NonZeroUsize;
use core::ptr::null_mut;
use core::task::Waker;

// This linked list come from https://github.com/async-rs/async-std/pull/370 by nbdd0121

struct WakerNode {
    /// Previous `WakerNode` in the queue. If this node is the first node, it shall point to the last node.
    prev_in_queue: *mut WakerNode,
    /// Next `WakerNode` in the queue. If this node is the last node, it shall be null.
    next_in_queue: *mut WakerNode,
    waker: Option<Waker>,
}

pub(crate) struct WakerList {
    head: *mut WakerNode,
}

unsafe impl Send for WakerList {}

unsafe impl Sync for WakerList {}

impl WakerList {
    /// Create a new empty `WakerList`
    pub(crate) fn new() -> Self {
        Self { head: null_mut() }
    }

    /// Insert a waker to the back of the list, and return its key.
    pub(crate) fn insert(&mut self, waker: Option<Waker>) -> NonZeroUsize {
        let node = Box::into_raw(Box::new(WakerNode {
            waker,
            next_in_queue: null_mut(),
            prev_in_queue: null_mut(),
        }));

        if self.head.is_null() {
            unsafe {
                (*node).prev_in_queue = node;
            }
            self.head = node;
        } else {
            unsafe {
                let prev = replace(&mut (*self.head).prev_in_queue, node);
                (*prev).next_in_queue = node;
                (*node).prev_in_queue = prev;
            }
        }

        unsafe { NonZeroUsize::new_unchecked(node as usize) }
    }

    /// Remove a waker by its key.
    ///
    /// # Safety
    /// This function is unsafe because there is no guarantee that key is the previously returned
    /// key, and that the key is only removed once.
    pub(crate) unsafe fn remove(&mut self, key: NonZeroUsize) -> Option<Waker> {
        let node = key.get() as *mut WakerNode;
        let prev = (*node).prev_in_queue;
        let next = (*node).next_in_queue;

        // Special treatment on removing first node
        if self.head == node {
            self.head = next;
        } else {
            (*prev).next_in_queue = next;
        }

        // Special treatment on removing last node
        if next.is_null() {
            if !self.head.is_null() {
                (*self.head).prev_in_queue = prev;
            }
        } else {
            (*next).prev_in_queue = prev;
        }

        Box::from_raw(node).waker
    }

    /// Get a waker by its key.
    ///
    /// # Safety
    /// This function is unsafe because there is no guarantee that key is the previously returned
    /// key, and that the key is not removed.
    pub(crate) unsafe fn get(&mut self, key: NonZeroUsize) -> &mut Option<Waker> {
        &mut (*(key.get() as *mut WakerNode)).waker
    }

    /// Get an iterator over all wakers.
    fn iter_mut(&mut self) -> Iter<'_> {
        Iter {
            ptr: self.head,
            _marker: PhantomData,
        }
    }

    /// Take the first not `None` waker in the list and wake it.
    pub(crate) fn wake_one(&mut self) {
        for opt in self.iter_mut() {
            if let Some(waker) = opt.take() {
                return waker.wake();
            }
        }
    }
}

pub(crate) struct Iter<'a> {
    ptr: *mut WakerNode,
    _marker: PhantomData<&'a ()>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a mut Option<Waker>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr.is_null() {
            return None;
        }
        let next = unsafe { (*self.ptr).next_in_queue };
        let ptr = replace(&mut self.ptr, next);
        Some(unsafe { &mut (*ptr).waker })
    }
}

/// a helper trait for interacting with WakerListLock
pub(crate) trait ListAPI {
    fn head_as_usize(&self) -> usize;
    fn as_ref(&self) -> &WakerList;
    fn as_mut(&mut self) -> &mut WakerList;
    fn from_usize(value: usize) -> Self;
}

impl ListAPI for WakerList {
    fn head_as_usize(&self) -> usize {
        self.head as usize
    }

    fn as_ref(&self) -> &WakerList {
        &self
    }

    fn as_mut(&mut self) -> &mut WakerList {
        self
    }

    fn from_usize(value: usize) -> Self {
        WakerList {
            head: value as *mut WakerNode,
        }
    }
}

pub(crate) mod linked_list_lock {
    #[cfg(feature = "no-send")]
    use core::cell::Cell;
    use core::marker::PhantomData;
    use core::ops::{Deref, DerefMut};

    #[cfg(not(feature = "no-send"))]
    use {
        crate::util::backoff::Backoff,
        core::sync::atomic::{AtomicUsize, Ordering},
    };

    use super::{ListAPI, WakerList};

    /// `WakerListLock` is a spin lock by default. It stores the head of the LinkedList
    /// In `no-send` feature it is a wrapper of `Cell` where the thread will panic if concurrent access happen
    macro_rules! waker_list_lock {
        ($lock_type: ty) => {
            pub(crate) struct WakerListLock<T: ListAPI> {
                head: $lock_type,
                _p: PhantomData<T>,
            }

            pub(crate) struct WakerListGuard<'a, T: ListAPI> {
                head: &'a $lock_type,
                list: T,
            }

            impl<T: ListAPI> WakerListLock<T> {
                pub(crate) fn new(list: T) -> Self {
                    Self {
                        head: <$lock_type>::new(list.head_as_usize()),
                        _p: PhantomData,
                    }
                }
            }
        };
    }

    #[cfg(not(feature = "no-send"))]
    waker_list_lock!(AtomicUsize);
    #[cfg(feature = "no-send")]
    waker_list_lock!(Cell<usize>);

    impl<'a, T: ListAPI> Deref for WakerListGuard<'a, T> {
        type Target = WakerList;

        fn deref(&self) -> &WakerList {
            self.list.as_ref()
        }
    }

    impl<'a, T: ListAPI> DerefMut for WakerListGuard<'a, T> {
        fn deref_mut(&mut self) -> &mut WakerList {
            self.list.as_mut()
        }
    }

    #[cfg(not(feature = "no-send"))]
    impl<'a, T: ListAPI> Drop for WakerListGuard<'a, T> {
        fn drop(&mut self) {
            self.head
                .store(self.list.head_as_usize(), Ordering::Release);
        }
    }

    #[cfg(feature = "no-send")]
    impl<'a, T: ListAPI> Drop for WakerListGuard<'a, T> {
        fn drop(&mut self) {
            self.head.set(self.list.head_as_usize());
        }
    }

    #[cfg(not(feature = "no-send"))]
    impl<T: ListAPI> WakerListLock<T> {
        pub(crate) fn lock(&self) -> WakerListGuard<'_, T> {
            let backoff = Backoff::new();
            loop {
                let value = self.head.swap(1, Ordering::Acquire);
                if value != 1 {
                    return WakerListGuard {
                        head: &self.head,
                        list: T::from_usize(value),
                    };
                }
                backoff.snooze();
            }
        }
    }

    #[cfg(feature = "no-send")]
    impl<T: ListAPI> WakerListLock<T> {
        pub(crate) fn lock(&self) -> WakerListGuard<'_, T> {
            let value = self.head.replace(1);

            if value != 1 {
                return WakerListGuard {
                    head: &self.head,
                    list: T::from_usize(value),
                };
            } else {
                panic!("WakerListLock already locked by others");
            }
        }
    }
}
