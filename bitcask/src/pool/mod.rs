mod channel;

use core::panic;
use std::{
    collections::{HashMap, VecDeque},
    io, mem,
    sync::{
        mpsc::{self, RecvError},
        Arc, Condvar, Mutex,
    },
    thread::{self, JoinHandle},
};

type BoxFn<'a> = Box<dyn FnOnce() + Send + 'a>;

#[derive(Debug)]
pub struct Job {}

struct Pool {
    inner: Inner,
    shutdown_rx: channel::Receiver,
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        todo!()
    }
}

struct Inner {
    shared: Mutex<Shared>,

    /// The condvar against which idle workers wait
    condvar: Condvar,
}

/// Shared data across all worker threads
struct Shared {
    /// The queue of pending jobs
    queue: VecDeque<BoxFn<'static>>,

    /// Number of active worker threads
    num_threads: usize,

    /// id of next worker thread that will be spawned
    thread_idx: usize,

    /// mapping of thread_id to their join handles
    worker_threads: HashMap<usize, JoinHandle<()>>,

    /// Number of currently waiting threads
    waiting_threads: usize,

    /// Flag set when pool is shutting down.
    shutdown: bool,

    // todo: replace with oneshot channel
    shutdown_tx: channel::Sender,
}

pub struct PoolHandle {
    handle: Arc<Pool>,
}

type Queue = Arc<Mutex<VecDeque<Job>>>;

impl Pool {
    pub fn new() -> Self {
        let (send, recv) = channel::channel();
        Self {
            inner: Inner {
                shared: Mutex::new(Shared {
                    queue: VecDeque::new(),
                    num_threads: 0,
                    thread_idx: 0,
                    worker_threads: HashMap::new(),
                    waiting_threads: 0,
                    shutdown: false,
                    shutdown_tx: send,
                }),
                condvar: Condvar::new(),
            },
            shutdown_rx: recv,
        }
    }

    pub fn execute<F>(&self, func: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let mut shared = self.inner.shared.lock().unwrap();
        shared.queue.push_back(Box::new(func));

        if shared.num_threads == 0 {
            let current_idx = shared.thread_idx;
            let shutdown_tx = shared.shutdown_tx.clone();

            match self.spawn_thread(&self, current_idx, shutdown_tx) {
                Ok(handle) => {
                    shared.thread_idx += 1;
                    shared.worker_threads.insert(current_idx, handle);
                }
                Err(e) => {
                    panic!("Error spawning thread in threadpool: {}", e);
                }
            }
        } else {
            shared.waiting_threads += 1;
            self.inner.condvar.notify_one();
        }
    }

    pub fn shutdown(&self) {
        let mut shared = self.inner.shared.lock().unwrap();

        if shared.shutdown {
            // Someone's already set the flag either through this function or in the drop method.
            // Exist early to prevent shutting down twice.
            return;
        }

        // First thread that enters this critical section is responsible for ensuring all current
        // threads exit.
        let workers = mem::take(&mut shared.worker_threads);
        // drop the lock to allow other threads to enther shutdown
        drop(shared);

        // When all existing threads have finished their run loops, we drop the send half, which
        // results in an err
        if let Err(_) = self.shutdown_rx.recv.recv() {
            for (_id, worker) in workers {
                let _ = worker.join();
            }
        }
    }

    fn spawn_thread(
        &self,
        handle: &Pool,
        id: usize,
        shutdown_tx: channel::Sender,
    ) -> io::Result<thread::JoinHandle<()>> {
        let builder = thread::Builder::new();
        let handle = handle.clone();

        builder.spawn(move || {
            handle.inner.run(id);

            // Drop the send half of the channel to signal that we're out of the core loop
            drop(shutdown_tx);
        })
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Inner {
    fn run(&self, thread_id: usize) {
        let mut shared = self.shared.lock().unwrap();

        // main worker thread loop
        loop {
            // Busy state
            // Grab the first available job in the queue
            while let Some(job) = shared.queue.pop_front() {
                // drop the mutex guard as we've obtained a job from the queue
                drop(shared);
                // todo: Use a channel to send result
                let _result = job();

                shared = self.shared.lock().unwrap();
            }

            // Idle
            while !shared.shutdown {
                // Wait until we get notified of a new job on the queue
                // todo: Use wait_timeout here?
                shared = self.condvar.wait(shared).unwrap();

                if shared.waiting_threads != 0 {
                    // We have more jobs to pick up. Decrement number of waiting threads and break
                    // into the busy part of the loop
                    shared.waiting_threads -= 1;
                    break;
                }

                // Spurious wakeup. Going back to sleep
            }

            // Shutdown
            if shared.shutdown {
                // Draining existing jobs from the queue without running them
                while let Some(_job) = shared.queue.pop_front() {
                    shared = self.shared.lock().unwrap();
                }

                break;
            }
        }
    }
}
