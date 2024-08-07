use std::{fmt::write, sync::Once, thread};

use anyhow::Result;
use bitcask::{test::TestFileSystem, Cask, Config, FileSystem};
use tracing::Level;

use pretty_assertions::assert_eq;

static TRACING: Once = Once::new();

fn init_tracing() {
    TRACING.call_once(|| {
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .init();
    });
}

#[test]
fn test_active_file_swap() -> Result<()> {
    init_tracing();

    let test_fs = <TestFileSystem as FileSystem>::init("")?;
    let cask: Cask<TestFileSystem> = Cask::new_with_fs_impl(
        "./",
        Config {
            active_threshold: 264,
        },
        test_fs.clone(),
    )?;

    for _ in 0..512 {
        cask.insert("entry", "1")?;
    }

    // Each entry requiring a header adds a lot of overhead
    // (Header (15 bytes) + Entry (6 + 1)) * 512 / 264
    assert_eq!(test_fs.num_files(), 40);

    assert_eq!(cask.get(&"entry")?, "1".as_bytes());

    Ok(())
}

#[test]
fn test_active_file_swap_multiple_threads() -> Result<()> {
    //init_tracing();

    let test_fs = <TestFileSystem as FileSystem>::init("")?;
    let cask: Cask<TestFileSystem> = Cask::new_with_fs_impl(
        "./",
        Config {
            active_threshold: 264,
        },
        test_fs.clone(),
    )?;

    let mut handles = vec![];
    for i in 0..2 {
        let new_cask = cask.clone();
        handles.push(
            thread::Builder::new()
                .name(format!("thread-{i}"))
                .spawn(move || {
                    for _ in 0..100 {
                        new_cask
                            .insert("entry", "1")
                            .expect("Unable to insert into the datastore");
                    }
                    println!("Inserted entry 512 times in thread-{i}");
                }),
        );
    }

    for handle in handles {
        let _ = handle?.join();
    }

    // Each entry requiring a header adds a lot of overhead
    // (Header (15 bytes) + Entry (5 + 1)) * 1000 / 264
    assert_eq!(test_fs.num_files(), 79);

    assert_eq!(cask.get(&"entry")?, "1".as_bytes());

    Ok(())
}
