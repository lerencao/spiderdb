use super::*;
use failure::Error;

pub struct BlockIterator<'a, 'b: 'a> {
    block: &'a Block<'b>,
    pos: u32, // position in block's data
    base_key: Vec<u8>,
    last: Option<Result<Header, Error>>,
}

impl<'a, 'b: 'a> From<&'a Block<'b>> for BlockIterator<'a, 'b> {
    fn from(block: &'a Block<'b>) -> Self {
        BlockIterator {
            block,
            pos: 0,
            base_key: vec![],
            last: None,
        }
    }
}

impl<'a, 'b: 'a> BlockIterator<'a, 'b> {
    pub fn reset(&mut self) {
        self.pos = 0;
        self.base_key = vec![];
        self.last = None;
    }

    // Parse next header-k-v
    fn parse_next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if (self.pos as usize) == self.block.len() {
            return Option::None;
        } else if self.pos as usize > self.block.len() {
            panic!("This should not happen")
        }
        let header_res = self.parse_header();
        match header_res {
            Err(e) => {
                self.last = Some(Err(e));
                None
            }
            Ok(header) if header.klen == 0 && header.plen == 0 => Option::None,
            Ok(header) => {
                // Populate baseKey if it isn't set yet. This would only happen for the first Next.
                if self.base_key.is_empty() {
                    // This should be the first Next() for this block. Hence, prefix length should be zero.
                    assert_eq!(header.plen, 0);
                    let key: &[u8] = &self.block.data
                        [(self.pos as usize)..(self.pos as usize + header.klen as usize)];
                    self.base_key = key.to_vec();
                }
                match self.parse_kv(&header) {
                    Ok(kv) => Some(kv),
                    Err(e) => {
                        self.last = Some(Err(e));
                        None
                    }
                }
            }
        }
    }

    // parseKV would allocate a new byte slice for key and for value.
    fn parse_kv(&mut self, header: &Header) -> Result<(Vec<u8>, Vec<u8>), Error> {
        self.parse_key(header)
            .and_then(|k| self.parse_value(header).map(|v| (k, v)))
    }

    // The caller should make sure that the `pos` is less than the `block.len()`
    fn parse_header(&mut self) -> Result<Header, Error> {
        let mut slice: &[u8] = &self.block.data[self.pos as usize..];
        let header = Header::decode(&mut slice)?;
        self.pos += Header::SIZE as u32;

        Ok(header)
    }
    fn parse_key(&mut self, header: &Header) -> Result<Vec<u8>, Error> {
        // TODO: should we shrunk the capacity to `header.plen + header.ken`
        let mut key: Vec<u8> = vec![];

        // make sure `header.plen` is not greater than len of base_key
        assert!(header.plen as usize <= self.base_key.len());
        key.extend_from_slice(&self.base_key[0..header.plen as usize]);

        assert!(self.pos as usize <= self.block.len() - 1);
        let diff_key_range = self.pos as usize..self.pos as usize + header.klen as usize;
        if diff_key_range.end > self.block.len() {
            return Err(DecodeError::KeyExceedSizeOfBlock {
                pos: self.pos,
                block_len: self.block.len() as u32,
                h: *header,
            })?;
        }
        let diff_key: &[u8] = &self.block.data[diff_key_range];
        self.pos += header.klen as u32;
        key.extend_from_slice(diff_key);

        Ok(key)
    }

    fn parse_value(&mut self, header: &Header) -> Result<Vec<u8>, Error> {
        let value_range = self.pos as usize..self.pos as usize + header.vlen as usize;
        if value_range.end > self.block.len() {
            // TODO: record this error.
            Err(DecodeError::ValueExceedSizeOfBlock {
                pos: self.pos,
                block_len: self.block.data.len() as u32,
                h: *header,
            })?
        } else {
            let value: &[u8] = &self.block.data[value_range];
            Ok(value.to_vec())
        }
    }
}

impl<'a, 'b: 'a> Iterator for BlockIterator<'a, 'b> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.last {
            Some(Err(_)) => None,
            _ => self.parse_next(),
        }
    }
}

pub enum SeekFrom {
    Start,
    Current,
}

impl<'a, 'b: 'a> BlockIterator<'a, 'b> {
    // seek the first key that >= prefix
    pub fn seek(&mut self, prefix: &[u8], from: SeekFrom) {
        self.last = None;
        match from {
            SeekFrom::Start => self.reset(),
            _ => {},
        }
        let found = self.find(|kv| {
            let key: &[u8] = &kv.0;
            key >= prefix
        });
    }
}

struct TableIterator<'a> {
    t: &'a Table,
    reversed: bool,
    bpos: u32,
}

impl <'a> TableIterator<'a> {
    pub fn new(t: &'a Table, reversed: bool) -> TableIterator<'a> {
        TableIterator {
            t,
            reversed,
            bpos: 0,
        }
    }

    pub fn reset(&mut self) {
        self.bpos = 0;
    }
}
