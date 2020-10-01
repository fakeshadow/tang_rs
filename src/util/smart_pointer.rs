#[cfg(feature = "no-send")]
pub(crate) use std::rc::{Rc as RefCounter, Weak as WeakRefCounter};
#[cfg(not(feature = "no-send"))]
pub(crate) use std::sync::{Arc as RefCounter, Weak as WeakRefCounter};
