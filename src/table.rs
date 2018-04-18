use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use failure::Error;
use memmap;
use memmap::Mmap;
use std::fmt;
use std::fmt::Formatter;
use std::fs;
use std::fs::File;
use std::fs::Metadata;
use std::io;
use std::io::Cursor;
use std::io::Read;


pub struct Table {
    id: u64,
    fd: File,
    table_size: u64,
    mmap: Mmap,
    block_index: Vec<KeyOffset>,
}

struct KeyOffset {
    offset: usize,
    len: usize,
}

pub enum TableLoadMode {
    LoadToRAM,
    MemoryMap,
    //    FileIO
}

impl Table {
    pub fn open(file_id: u64, mut fd: fs::File, load_mode: TableLoadMode) -> io::Result<Table> {
        let meta: Metadata = fd.metadata()?;
        let initial_len = meta.len();

        let mmap: Mmap = match load_mode {
            TableLoadMode::LoadToRAM => {
                let mut mmap = memmap::MmapOptions::new()
                    .len(initial_len as usize)
                    .map_anon()?;
                fd.read_exact(&mut mmap)?;
                mmap.make_read_only()?
            }
            TableLoadMode::MemoryMap => {
                let mmap = unsafe { memmap::MmapOptions::new().map(&fd) }?;
                mmap
            }
        };
        let block_index = Table::read_index(&mmap)?;
        let table = Table {
            id: file_id,
            table_size: mmap.len() as u64,
            fd,
            mmap,
            block_index,
        };

        Ok(table)
    }

    // TODO: impl Index<Block> instead of it.
    // Need to track self referential struct.
    pub fn block<'a>(&'a self, index: usize) -> io::Result<Block<'a>> {
        let bi = &self.block_index[index];
        let data = Table::read_mmap(&self.mmap, bi.offset, bi.len).map(|da| Block {
            offset: bi.offset as u32,
            data: da,
        });
        data
    }

    pub fn size(&self) -> u64 {
        self.table_size
    }
    pub fn id(&self) -> u64 {
        self.id
    }

    fn read_index(mmap: &Mmap) -> io::Result<Vec<KeyOffset>> {
        let mut read_pos = mmap.len() as u64;
        // read bloom size
        read_pos -= 4;
        let bloom_len = {
            let buf = Table::read_mmap(mmap, read_pos as usize, 4)?;
            let mut cur = Cursor::new(buf);
            cur.read_u32::<BigEndian>()?
        };
        // read bloom
        read_pos -= bloom_len as u64;
        let bloom_buf = {
            let buf = Self::read_mmap(mmap, read_pos as usize, bloom_len as usize)?;
            buf
        };
        // read restart len
        read_pos -= 4;
        let restart_len: usize = {
            let mut buf = Self::read_mmap(mmap, read_pos as usize, 4)?;
            buf.read_u32::<BigEndian>()? as usize
        };
        read_pos -= 4 * (restart_len as u64);
        let mut offsets_buf = Table::read_mmap(mmap, read_pos as usize, 4 * restart_len)?;

        let mut prev = 0;
        let mut block_index = Vec::with_capacity(restart_len);
        for i in 0..restart_len {
            let off = offsets_buf.read_u32::<BigEndian>()?;
            block_index[i] = KeyOffset {
                offset: prev as usize,
                len: (off - prev) as usize,
            };
            prev = off;
        }

        Ok(block_index)
    }

    fn read_mmap(mmap: &[u8], offset: usize, size: usize) -> io::Result<&[u8]> {
        if mmap.len() < offset + size {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        } else {
            return Ok(&mmap[offset..offset + size]);
        }
    }
}

#[derive(Debug, Fail)]
enum DecodeError {
    #[fail(display = "Value exceeded size of block: pos({}) block_len({}) header({})", pos,
           block_len, h)]
    ValueExceedSizeOfBlock { pos: u32, h: Header, block_len: u32 },
    #[fail(display = "Key exceeded size of block: pos({}) block_len({}) header({})", pos,
           block_len, h)]
    KeyExceedSizeOfBlock { pos: u32, h: Header, block_len: u32 },
}

#[derive(Default, Copy, Clone, Debug)]
struct Header {
    plen: u16, // Overlap with base key.
    klen: u16, // Length of the diff.
    vlen: u16, // Length of value.
    // Offset for the previous key-value pair. The offset is relative to block base offset.
    prev: u32,
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "plen:{}, klen:{}, vlen:{}, prev:{}",
            self.plen, self.klen, self.vlen, self.prev
        )
    }
}

impl Header {
    pub const SIZE: u16 = 16;

    pub fn decode<T: ReadBytesExt>(reader: &mut T) -> io::Result<Header> {
        Ok(Header {
            plen: reader.read_u16::<BigEndian>()?,
            klen: reader.read_u16::<BigEndian>()?,
            vlen: reader.read_u16::<BigEndian>()?,
            prev: reader.read_u32::<BigEndian>()?,
        })
    }
}

pub struct Block<'a> {
    offset: u32,
    data: &'a [u8],
}

impl<'a> Block<'a> {
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

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

impl<'a, 'b: 'a> DoubleEndedIterator for BlockIterator<'a, 'b> {
    fn next_back(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn assert_always_true() {
        assert!(true)
    }
}
