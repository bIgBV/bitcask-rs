use bitcask::{Cask, Config, test::TestFileSystem};
use anyhow::Result;

#[test]
fn test_active_file_swap() -> Result<()> {
    let cask: Cask<TestFileSystem> = Cask::new_with_config("./", Config {
        active_threshold: 265
    })?;

    for i in 0..512 {
        cask.insert("entry", format!("{}", i))?;
    }

    Ok(())
}
