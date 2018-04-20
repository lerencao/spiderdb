#![feature(test)]
#![feature(rustc_private)]
extern crate spiderdb;
extern crate tempdir;
extern crate test;
extern crate rand;

use test::Bencher;
use spiderdb::values::values::{ValueLog, ValueOption};
use spiderdb::values::write::Value;
use rand::{Rand, StdRng, Rng};
use std::path;
#[bench]
fn bench_value_write_sync(b: &mut Bencher) {
    let tmp_dir = tempdir::TempDir::new("value_log").unwrap().into_path();
    let mut vl = ValueLog::open(&ValueOption::new(&tmp_dir, 1024 * 1024 * 96, true)).unwrap();
    let mut rng = rand::thread_rng();

    b.iter( || {
        let key: Vec<u8> = rng.gen_iter().take(100).collect();
        let value: Vec<u8> = rng.gen_iter().take(10000).collect();
        let pointers = vl.write(&[Value::new(&key, &value)]).unwrap();
        for p in &pointers {
            vl.read(p).unwrap();
        }
    });
}

#[bench]
fn bench_value_write_nosync(b: &mut Bencher) {
    let tmp_dir = tempdir::TempDir::new("value_log").unwrap().into_path();
    let mut vl = ValueLog::open(&ValueOption::new(&tmp_dir, 1024 * 1024 * 96, false)).unwrap();
    let mut rng = rand::thread_rng();

    b.iter( || {
        let key: Vec<u8> = rng.gen_iter().take(100).collect();
        let value: Vec<u8> = rng.gen_iter().take(10000).collect();
        let pointers = vl.write(&[Value::new(&key, &value)]).unwrap();
        for p in &pointers {
            vl.read(p).unwrap();
        }
    });
}

