use std::{sync::Arc, thread};

use argh::FromArgs;
use tracing::Level;

use bitcask::{Cask, ConcreteSystem};

#[derive(Debug, FromArgs)]
/// What it says on the tin: runs simple programs for testing bitcask
struct Opts {
    #[argh(switch)]
    /// emit debug info
    debug: bool,

    #[argh(option, default = "4")]
    /// number of threads to spawn
    num_threads: usize,
}

fn main() {
    let opts: Opts = argh::from_env();

    if opts.debug {
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .init();
    }

    let cask: Cask<ConcreteSystem> = Cask::new("./").unwrap();
    let cask = Arc::new(cask);

    let mut handles = Vec::with_capacity(opts.num_threads);

    for thread in 0..opts.num_threads {
        let cask = cask.clone();
        let handle = thread::spawn(move || {
            for i in 0..10000 {
                let key = format!("hello{i}");
                let value = format!("world {i}");
                cask.insert(key, value).unwrap();

                // Re-create key as we move it into the cask
                let key = format!("hello{i}");
                let result = cask.get(&key).unwrap();
                let val = String::from_utf8(result).unwrap();
                // println!("thread-{thread}: {val}");
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}
