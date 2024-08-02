use std::sync::{Arc, Once};

use anyhow::Result;
use bitcask::{test::TestFileSystem, Cask, Config, FileSystem, System};
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
            active_threshold: 265,
        },
        test_fs.clone(),
    )?;

    for i in 0..512 {
        cask.insert("entry", format!("{}", i))?;
    }

    assert_eq!(test_fs.num_files(), 2);

    Ok(())
}
