#[cfg(feature = "no-send")]
pub(crate) mod cell_pool;
pub(crate) mod linked_list;
#[cfg(not(feature = "no-send"))]
pub(crate) mod spin_pool;
pub(crate) mod timeout;
