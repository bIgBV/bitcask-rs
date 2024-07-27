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
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use crate::repr::{Entry, Header};

enum State {
    /// Stores the instant when we went into the wait state, along with the current instant
    Wait(Instant),

    /// Compact state
    Compact,
}

#[derive(Debug)]
pub(crate) enum Operation<'entry> {
    Ignore,
    CheckFile,
    CheckKeydir(&'entry [u8]),
    AddImmutable,
    AddHint,
}

pub(crate) struct Compactor<'entry> {
    operations: VecDeque<Operation<'entry>>,
    state: State,
}

pub(crate) enum Input<'file> {
    Entry(Entry<'file>),
    End(Instant),
    MatchKeydir,
    NotMatchkeydir,
}

impl<'entry> Compactor<'entry> {
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

    pub fn handle_input(&mut self, input: Input<'entry>) {
        match self.state {
            // Don't need to do anything in this state.
            State::Wait(_at) => {}

            State::Compact => {
                // If the file exists and entries are present, we are actively compacting
                match input {
                    Input::Entry(entry) => {
                        //if entry.is_tombstone() {
                        //    self.operations.push_back(Operation::Ignore);
                        //} else {
                        //    self.operations
                        //        .push_back(Operation::CheckKeydir(entry.key()));
                        //}
                    }
                    Input::MatchKeydir => {
                        self.operations.push_back(Operation::AddImmutable);
                        self.operations.push_back(Operation::AddHint)
                    }
                    Input::NotMatchkeydir => self.operations.push_back(Operation::Ignore),
                    // We're reached the end of the file, switch to the wait state
                    Input::End(now) => self.state = State::Wait(now),
                }
            }
        }
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        let last_sleep = match self.state {
            State::Wait(at) => at,
            State::Compact => return,
        };

        if last_sleep.duration_since(now) < Duration::from_secs(60 * 60) {
            return;
        }

        self.operations.push_back(Operation::CheckFile);
        self.state = State::Compact;
    }

    pub fn poll_transmit(&mut self) -> Option<Operation> {
        self.operations.pop_front()
    }

    pub fn poll_timeout(&self) -> Option<Instant> {
        match self.state {
            State::Compact => None,
            State::Wait(at) => Some(at + Duration::from_secs(60 * 60)),
        }
    }
}
