mod cask;

use crate::cask::Cask;

fn main() {
    tracing_subscriber::fmt().init();
    let mut cask = Cask::new("./").unwrap();
    cask.insert("hello", "world 🧏‍♀️").unwrap();
    let result = cask.get("hello").unwrap();

    let val = String::from_utf8(result).unwrap();
    println!("{val}");
}
