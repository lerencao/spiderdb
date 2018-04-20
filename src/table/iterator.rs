use super::*;
use failure::Error;
use std::convert::From;

impl<'a> IntoIterator for Block<'a> {
    type Item = (Vec<u8>, Vec<u8>);
    type IntoIter = BlockIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        BlockIterator {
            block: self,
            pos: 0,
            base_key: vec![],
            last: None,
        }
    }
}

pub struct BlockIterator<'b> {
    block: Block<'b>,
    pos: u32, // position in block's data
    base_key: Vec<u8>,
    last: Option<Result<Header, Error>>,
}

impl<'a> BlockIterator<'a> {
    // get err if errored.
    pub fn err(&self) -> Option<&Error> {
        self.last.as_ref().and_then(|l| l.as_ref().err())
    }
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

impl<'a> Iterator for BlockIterator<'a> {
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

impl<'a> BlockIterator<'a> {
    // seek the first key that >= prefix
    pub fn seek(&mut self, prefix: &[u8], from: SeekFrom) {
        self.last = None;
        match from {
            SeekFrom::Start => self.reset(),
            _ => {}
        }
        let found = self.find(|kv| {
            let key: &[u8] = &kv.0;
            key >= prefix
        });
    }
}

impl Table {
    pub fn iter(&self) -> TableIterator {
        TableIterator::new(self)
    }
}

pub struct TableIterator<'a> {
    t: &'a Table,
    block_pos: u32,
    block_iter: Option<BlockIterator<'a>>,
    err: Option<Error>,
}

impl<'a> TableIterator<'a> {
    pub fn new(t: &'a Table) -> TableIterator<'a> {
        TableIterator {
            t,
            block_pos: 0,
            block_iter: None,
            err: None,
        }
    }

    // Reset iterator to the beginning.
    pub fn reset(&mut self) {
        self.block_pos = 0;
        self.block_iter = None;
        self.err = None;
    }

    // Get err if any error occurred
    pub fn err(&self) -> Option<&Error> {
        self.err
            .as_ref()
            .or_else(|| self.block_iter.as_ref().and_then(|bi| bi.err()))
    }
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        // Check if error occurred.
        if self.err().is_some() {
            return None;
        }

        // Check if no block left.
        if self.block_pos as usize >= self.t.block_index.len() {
            return None;
        }
        if self.block_iter.is_none() {
            let block_res = self.t.block(self.block_pos as usize);
            let block_iter = block_res.unwrap().into_iter();
            self.block_iter = Some(block_iter);
        }
        let item = self.block_iter.as_mut().unwrap().next();
        if item.is_some() {
            item
        } else {
            self.block_pos += 1;
            self.block_iter = None;
            self.next()
        }
    }
}
