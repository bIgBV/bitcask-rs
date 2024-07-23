#[cfg(loom)]
pub(crate) use loom::{
    sync::{atomic::AtomicUsize, Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

#[cfg(not(loom))]
pub(crate) use std::{
    sync::{atomic::AtomicUsize, Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};
