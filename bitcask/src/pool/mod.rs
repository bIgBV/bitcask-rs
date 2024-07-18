use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    thread::{self, Thread},
};

use crate::fs::Fd;

#[derive(Debug)]
pub struct Job {
    fd: Fd,
}

#[derive(Debug)]
struct Pool {
    queue: Mutex<VecDeque<Job>>,
    waiters: Mutex<Vec<Thread>>,
}

pub struct PoolHandle {
    handle: Arc<Pool>,
}

type Queue = Arc<Mutex<VecDeque<Job>>>;

impl Pool {
    pub fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            waiters: Mutex::new(Vec::new()),
        }
    }

    pub fn add_job(&self, job: Job) {
        self.queue.lock().unwrap().push_back(job)
    }

    fn run_job(&self, queue: &Queue) {
        if let Some(job) = queue.lock().unwrap().pop_front() {
            println!("Processing {job:?}");
        } else {
            let handle = thread::current();
            self.waiters.lock().unwrap().push(handle);
        }
    }
}
