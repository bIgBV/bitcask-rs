//! Sans-io state machine controlling the compaction loop
//!
//! The compactor performs the following set of operations:
//! - Loop on an immutable file
//! - Check if entry is present in KeyDir
//!     - If it's present,
//!         - and it's timestamp is the same as the one in the keydir entry
//!         - and it's location matches KeyDir location, move it to compacted file.
//!         - Also create a new hintfile entry
//!     - If it is not the same location, ignore this entry
//!     - If it's a tombstone entry, ignore
use std::{collections::VecDeque, time::Instant};

use crate::repr::{Entry, Header};

enum State {
    Wait(Instant),
    Compact,
}

#[derive(Debug)]
pub(crate) enum Operation<'file> {
    Ignore,
    CheckFile,
    AddImmutable(Entry<'file>),
    AddHint(Header),
}

pub(crate) struct Compactor<'file> {
    operations: VecDeque<Operation<'file>>,
    state: State,
}

impl<'file> Compactor<'file> {
    pub fn new() -> Self {
        let mut queue = VecDeque::new();
        queue.push_back(Operation::CheckFile);
        Self {
            operations: queue,
            // When compactor is initialized, we start in the loop state and are ready to issue a
            // CheckFile request as soon as we are polled.
            state: State::Compact,
        }
    }

    pub fn handle_input(&self, entry: Option<Entry<'file>>) {
        match self.state {
            State::Wait(_) => {}
            State::Compact => {
                // If the file exists and entries are present, we are actively compacting
                if let Some(entry) = entry {}
            }
        }
    }

    pub fn handle_timeout(&self, now: Instant) {}

    pub fn poll_transmit(&mut self) -> Option<Operation<'file>> {
        self.operations.pop_front()
    }

    pub fn poll_timeout(&self) -> Option<Instant> {
        todo!()
    }
}
