use tracing::Level;

use bitcask::{Cask, SysFileSystem};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .init();

    let cask: Cask<SysFileSystem> = Cask::new("./").unwrap();

    for i in 0..10 {
        let key = format!("hello{i}");
        let value = format!("world {i}");
        cask.insert(key, value).unwrap();

        // Re-create key as we move it into the cask
        let key = format!("hello{i}");
        let result = cask.get(&key).unwrap();
        let val = String::from_utf8(result).unwrap();
        println!("{val}");
    }

    cask.remove(&format!("hello3")).unwrap();
    assert!(cask.get(&format!("hello3")).is_err());
}
