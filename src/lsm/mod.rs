extern crate bincode;
extern crate serde;
extern crate skiplist;

use failure;
use self::bincode::config;
use self::header::*;
use self::serde::Serialize;
use self::skiplist::SkipMap;
use std;
use std::io::Write;

mod header;


type Key = Vec<u8>;


pub struct LSM<V> {
    mt: SkipMap<Key, V>,
}

impl<V> LSM<V> where {
    pub fn new(max_size: u32) -> LSM<V> {
        let mt = SkipMap::with_capacity(1024 * 1024);
        LSM {
            mt
        }
    }

    pub fn write(&mut self, k: Key, v: V) -> Option<V> {
        self.mt.insert(k, v)
    }

    pub fn get(&self, k: &Key) -> Option<&V> {
        self.mt.get(k)
    }

    fn flush_mt<W>(mt: SkipMap<Key, V>, mut w: W) -> std::result::Result<(), failure::Error>
        where V: Serialize, W: Write {
        let mut config = config();
        config.big_endian().no_limit();

        let restart_interval = 100u32; // number of kv in each blocks.

        // buf for a single skip map
        // TODO: init with capacity.
        let mut buf: Vec<u8> = vec![];
        // split the ordered skip list into blocks,
        // keys in each block share a same key prefix to reduce space.
        let mut base_key: Vec<u8> = vec![]; // the same key prefix for current block
        let mut base_offset = 0u32; // offset in the file for the current block
        let mut restarts: Vec<u32> = vec![]; // base offset for each block
        let mut prev_offset: Option<u32> = None; // track offset for previous kv pair. Offset is relative to block based offset.
        // number of kv written for the current block, init to restart_interval to fake restart for the first kv
        let mut counter = restart_interval;
        let mut restart = true;
        for (ref k, ref v) in mt.into_iter() {
            if (counter >= restart_interval) {
                counter = 0;
                base_key = k.clone();
                base_offset = buf.len() as u32;
                restarts.push(base_offset);
                prev_offset = None; // should set prev_offset to prev block's last kv
            }


            let lcp = lcp(&k, &base_key);
            let diff_k = &k[lcp.len()..];
            let vlen = config.serialized_size(v)? as u32;
            let header = Header {
                plen: base_key.len() as u16,
                klen: diff_k.len() as u16,
                vlen: vlen,
                prev: prev_offset,
            };
            config.serialize_into(&mut buf, &header)?;
            config.serialize_into(&mut buf, diff_k)?;
            config.serialize_into(&mut buf, v)?;

            counter += 1;
        }

        // write buf all in one to reduce disk io.
        w.write_all(&buf)?;
        w.flush()?;
        Ok(())
    }
}

fn lcp<'a, T>(v1: &'a [T], v2: &'a [T]) -> &'a [T] where T: PartialEq {
    let min_len = v1.len().min(v2.len());
    let mut max_len = min_len;
    for i in 0..min_len {
        if v1[i] != v2[i] {
            max_len = i;
            break;
        }
    }
    &v1[0..max_len]
}

#[cfg(test)]
mod test {
    #[test]
    fn test_lcp() {
        assert_eq!(b"", super::lcp(b"abc", b""));
        assert_eq!(b"ab", super::lcp(b"abc", b"ab"));
        assert_eq!(b"abc", super::lcp(b"abcd", b"abc"));
        assert_eq!(b"", super::lcp(b"babcd", b"abe"));
    }
}
