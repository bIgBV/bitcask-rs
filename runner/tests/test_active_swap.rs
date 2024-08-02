use std::sync::Once;

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
