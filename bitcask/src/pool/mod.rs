use core::panic;
use std::{
    collections::{HashMap, VecDeque},
    io,
    sync::{Arc, Condvar, Mutex},
    thread::{self, JoinHandle},
};

type BoxFn<'a, R> = Box<dyn FnOnce() -> R + Send + 'a>;

#[derive(Debug)]
pub struct Job {}

struct Pool<R> {
    inner: Inner<R>,
}

impl<R> Clone for Pool<R> {
    fn clone(&self) -> Self {
        todo!()
    }
}

struct Inner<R> {
    shared: Mutex<Shared<R>>,

    /// The condvar against which idle workers wait
    condvar: Condvar,
}

/// Shared data across all worker threads
struct Shared<R> {
    /// The queue of pending jobs
    queue: VecDeque<BoxFn<'static, R>>,

    /// Number of active worker threads
    num_threads: usize,

    /// id of next worker thread that will be spawned
    thread_idx: usize,

    /// mapping of thread_id to their join handles
    worker_threads: HashMap<usize, JoinHandle<()>>,
}

pub struct PoolHandle<R> {
    handle: Arc<Pool<R>>,
}

type Queue = Arc<Mutex<VecDeque<Job>>>;

impl<R> Pool<R> {
    pub fn new() -> Self {
        Self {
            inner: Inner {
                shared: Mutex::new(Shared {
                    queue: VecDeque::new(),
                    num_threads: 0,
                    thread_idx: 0,
                    worker_threads: HashMap::new(),
                }),
                condvar: Condvar::new(),
            },
        }
    }

    pub fn execute<F>(&self, func: F)
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let mut shared = self.inner.shared.lock().unwrap();
        shared.queue.push_back(Box::new(func));

        if shared.num_threads == 0 {
        } else {
            let current_idx = shared.thread_idx;

            match self.spawn_thread(&self, current_idx) {
                Ok(handle) => {
                    shared.thread_idx += 1;
                    shared.worker_threads.insert(current_idx, handle);
                }
                Err(e) => {
                    panic!("Error spawning thread in threadpool: {}", e);
                }
            }
        }
    }

    fn spawn_thread(&self, handle: &Pool<R>, id: usize) -> io::Result<thread::JoinHandle<()>>
    where
        R: Send + 'static,
    {
        let builder = thread::Builder::new();
        let handle = handle.clone();

        builder.spawn(move || {
            handle.inner.run(id);
        })
    }
}

impl<R> Inner<R> {
    fn run(&self, thread_id: usize) {
        let mut shared = self.shared.lock().unwrap();

        // main worker thread loop
        'main: loop {
            // Busy state
            // Grab the first available job in the queue
            while let Some(job) = shared.queue.pop_front() {
                // drop the mutex guard as we've obtained a job from the queue
                drop(shared);
                // todo: Use a channel to send result
                let _result = job();

                shared = self.shared.lock().unwrap();
            }

            while shared.queue.len() == 0 {
                // Wait until we get notified of a new job on the queue
                // todo: Use wait_timeout here?
                let lock_result = self.condvar.wait(shared);
            }
        }
    }
}
