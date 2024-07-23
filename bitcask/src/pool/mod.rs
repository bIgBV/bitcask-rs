mod channel;
mod sync;

use crate::pool::sync::{
    thread::{self, JoinHandle},
    Arc, AtomicUsize, Condvar, Mutex,
};

use std::{
    collections::{HashMap, VecDeque},
    io, mem,
    sync::atomic::Ordering,
};

use tracing::{debug, info, instrument};

type BoxFn<'a> = Box<dyn FnOnce() + Send + 'a>;

pub(crate) struct Pool {
    inner: Arc<Inner>,
    shutdown_rx: Arc<channel::Receiver>,
}

impl Clone for Pool {
    fn clone(&self) -> Self {
        // Increment refcount
        self.inner.num_handles.fetch_add(1, Ordering::AcqRel);

        let inner = self.inner.clone();
        Pool {
            inner,
            shutdown_rx: self.shutdown_rx.clone(),
        }
    }
}

struct Inner {
    shared: Mutex<Shared>,

    /// The condvar against which idle workers wait
    condvar: Condvar,

    /// Maximum number of threads in this thread pool
    max_threads: usize,

    /// Tracks number of pool handles that currently exit
    num_handles: AtomicUsize,
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
    shutdown_tx: Option<channel::Sender>,
}

impl Pool {
    pub fn new(max_threads: usize) -> Self {
        let (send, recv) = channel::channel();
        Self {
            inner: Arc::new(Inner {
                shared: Mutex::new(Shared {
                    queue: VecDeque::new(),
                    num_threads: 0,
                    thread_idx: 0,
                    worker_threads: HashMap::new(),
                    waiting_threads: 0,
                    shutdown: false,
                    shutdown_tx: Some(send),
                }),
                condvar: Condvar::new(),
                max_threads,
                num_handles: AtomicUsize::new(1),
            }),
            shutdown_rx: Arc::new(recv),
        }
    }

    #[instrument(skip(self, func))]
    pub fn execute<F>(&self, func: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let mut shared = self.inner.shared.lock().unwrap();
        shared.queue.push_back(Box::new(func));

        if shared.num_threads == 0 || shared.waiting_threads == 0 {
            info!(
                num_threads = shared.num_threads,
                waiting_threads = shared.waiting_threads,
                "No thread available to take work"
            );

            if shared.num_threads == self.inner.max_threads {
                info!("We hit max thread cap");
            } else {
                info!("Spawning new thread to handle task");
                let current_idx = shared.thread_idx;
                if let Some(shutdown_tx) = shared.shutdown_tx.clone() {
                    match self.spawn_thread(current_idx, shutdown_tx) {
                        Ok(handle) => {
                            shared.num_threads += 1;
                            shared.thread_idx += 1;
                            shared.worker_threads.insert(current_idx, handle);
                        }
                        Err(e) => {
                            panic!("Error spawning thread in threadpool: {}", e);
                        }
                    }
                }
            }
        } else {
            info!("notifying idle threads");
            shared.waiting_threads += 1;
            self.inner.condvar.notify_one();
        }
    }

    #[instrument(skip(self), fields(thread=?thread::current().id()))]
    pub fn shutdown(&self) {
        if self.inner.num_handles.load(Ordering::Acquire) != 1 {
            // There are still handles to the pool out there. Wait until we're the last
            debug!("More handles exist");
            return;
        }
        info!("Shutting down pool");
        let mut shared = self.inner.shared.lock().unwrap();

        if shared.shutdown_tx.is_none() {
            debug!("Another thread already set shutdown state");
            // Someone's already set the flag either through this function or in the drop method.
            // Exist early to prevent shutting down twice.
            return;
        }

        info!("We are responsible for shutting down the pool");
        // First thread that enters this critical section is responsible for ensuring all current
        // threads exit.
        shared.shutdown = true;

        // Setting this to None triggers the `Drop` of the inner sender, as the threads are getting
        // a clone. If we don't set this, we will always end up blocking on the `recv`.
        shared.shutdown_tx = None;
        self.inner.condvar.notify_all();
        let workers = mem::take(&mut shared.worker_threads);
        // drop the lock to allow other threads to enther shutdown
        drop(shared);

        // Wake up any idle threads to let them know that we're shutting down.
        // When all existing threads have finished their run loops, we drop the send half, which
        // results in an err
        if let Err(_) = self.shutdown_rx.recv.recv() {
            debug!("All threads have exited core loop");
            for (_id, worker) in workers {
                let _ = worker.join();
            }
        }

        info!("Finished shutting down pool");
    }

    #[instrument(skip(self, shutdown_tx))]
    fn spawn_thread(
        &self,
        id: usize,
        shutdown_tx: channel::Sender,
    ) -> io::Result<thread::JoinHandle<()>> {
        let builder = thread::Builder::new();
        let pool_handle = self.clone();

        builder.spawn(move || {
            pool_handle.inner.run(id);

            info!(thread = id, "Finished inner loop");
            // Drop the send half of the channel to signal that we're out of the core loop
            drop(shutdown_tx);
        })
    }
}

impl Drop for Pool {
    #[instrument(skip(self))]
    fn drop(&mut self) {
        self.inner.num_handles.fetch_sub(1, Ordering::Release);
        self.shutdown();
    }
}

impl Inner {
    #[instrument(skip(self))]
    fn run(&self, thread_id: usize) {
        let mut shared = self.shared.lock().unwrap();

        // main worker thread loop
        loop {
            // Busy state
            // Grab the first available job in the queue
            while let Some(job) = shared.queue.pop_front() {
                debug!("Popped job from queue");
                // drop the mutex guard as we've obtained a job from the queue
                drop(shared);
                // todo: Use a channel to send result
                let _result = job();

                shared = self.shared.lock().unwrap();
            }

            // Idle
            while !shared.shutdown {
                debug!("No more jobs, going to sleep");
                // Wait until we get notified of a new job on the queue
                // todo: Use wait_timeout here?
                shared = self.condvar.wait(shared).unwrap();

                if shared.waiting_threads != 0 {
                    debug!("new job added to queue. Transition to Busy");
                    // We have more jobs to pick up. Decrement number of waiting threads and break
                    // into the busy part of the loop
                    shared.waiting_threads -= 1;
                    break;
                }

                // Spurious wakeup. Going back to sleep
            }

            // Shutdown
            if shared.shutdown {
                debug!("Shutting down thread");
                // There are no jobs left _and_ we are shutting down
                // Draining existing jobs from the queue without running them
                while let Some(_job) = shared.queue.pop_front() {
                    shared = self.shared.lock().unwrap();
                }

                break;
            }
        }

        // Thread exit
        shared.num_threads -= 1;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{mpsc, Once},
        thread,
        time::Duration,
    };

    use crossbeam_channel::{self, unbounded};
    use tracing::{info, Level};

    use super::Pool;

    static TRACING: Once = Once::new();

    fn init_tracing() {
        TRACING.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(Level::TRACE)
                .init();
        });
    }

    #[test]
    fn simple_execution() {
        init_tracing();

        let n_jobs = 8;
        let pool = Pool::new(1);
        let (send, recv) = mpsc::channel();

        for i in 0..n_jobs {
            let send = send.clone();
            pool.execute(move || {
                thread::sleep(Duration::from_millis(1000));
                info!(thread = i, "Doing work");
                let _ = send.send(5);
            });
        }

        info!("Waiting for work");
        assert_eq!(recv.iter().take(n_jobs).fold(0, |acc, i| acc + i), 40);
    }

    #[cfg(loom)]
    #[test]
    fn loom_cloned_execution() {
        loom::model(|| {
            let n_jobs = 10;
            let pool = Pool::new(4);
            let (send, recv) = loom::sync::mpsc::channel();

            let send_copy = send.clone();
            let pool_copy = pool.clone();

            thread::spawn(move || {
                pool_copy.execute(move || {
                    info!(thread = "new", "Doing work");
                    let _ = send_copy.send(2);
                });
            });

            // Delay the next execution until we've had a chance to spawn the thread.
            thread::sleep(Duration::from_millis(500));
            for i in 0..n_jobs {
                let send = send.clone();
                pool.execute(move || {
                    thread::sleep(Duration::from_millis(1000));
                    info!(thread = i, "Doing work");
                    let _ = send.send(1);
                });
            }

            drop(send);
            info!("Waiting for work");

            let mut sum = 0;
            while let Ok(item) = recv.recv() {
                sum += item;
            }

            assert_eq!(sum, 12);
        });
    }
}
