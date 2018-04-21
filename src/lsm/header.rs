use std::fmt;

///! When dumping skip list into underline storage, `Header` save the meta of each-kv.
#[derive(Default, Copy, Clone, Debug, Deserialize, Serialize)]
pub struct Header {
    // TODO: custom serialize and deserialize
    pub plen: u16,
    // Overlap with base key.
    pub klen: u16,
    // Length of the diff.
    pub vlen: u32,
    // Length of value. u16 is not enough(max to 64k)
    // Offset for the previous key-value pair. The offset is relative to block base offset.
    pub prev: Option<u32>,
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "plen:{:?}, klen:{:?}, vlen:{:?}, prev:{:?}",
            self.plen, self.klen, self.vlen, self.prev
        )
    }
}
