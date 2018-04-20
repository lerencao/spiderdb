#![feature(option_filter)]
extern crate byteorder;
#[macro_use]
extern crate failure;
extern crate memmap;
extern crate serde;
#[macro_use]
extern crate serde_derive;

use std::fs;

pub mod table;
pub mod level;
pub mod txn;
pub mod values;

pub struct Config {
    dir: String,
    value_dir: String,
    sync_write: bool,
    table_loading_mode: u8,
    value_log_loading_mode: u8,
    max_table_size: u64,
}

pub struct DB {}

impl DB {
    fn open(cfg: &Config) {
        let meta = fs::metadata(&cfg.dir);
        fs::create_dir_all(&cfg.dir).unwrap();
        fs::create_dir_all(&cfg.value_dir).unwrap();
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
