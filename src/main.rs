mod cask;

use crate::cask::Cask;

fn main() {
    tracing_subscriber::fmt().init();
    let mut cask = Cask::new("./").unwrap();

    for i in 0..10 {
        //let key = format!("hello{i}");
        //let value = format!("world {i}");
        //cask.insert(key, value).unwrap();

        // Re-create key as we move it into the cask
        let key = format!("hello{i}");
        let result = cask.get(&key).unwrap();
        let val = String::from_utf8(result).unwrap();
        println!("{val}");
    }
}
