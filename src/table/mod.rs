pub mod iterator;
pub mod builder;
use byteorder::BigEndian;
use byteorder::ReadBytesExt;

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
    prefix: Vec<u8>,
    offset: u32,
    len: u32,
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
        let data = Table::read_mmap(&self.mmap, bi.offset as usize, bi.len as usize)
            .map(|da| Block { data: da });
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
        // TODO: construct bloom filter
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
                offset: prev,
                len: (off - prev),
                prefix: vec![],
            };
            prev = off;
        }

        // Read first key of a block, it's the prefix of this block.
        for ko in block_index.iter_mut() {
            let mut offset: usize = ko.offset as usize;
            let mut buf = Table::read_mmap(mmap, offset, Header::SIZE as usize)?;
            let header = Header::decode(&mut buf)?;
            assert_eq!(header.plen, 0);
            offset += Header::SIZE as usize;
            let key = Table::read_mmap(mmap, offset, header.klen as usize)?;
            ko.prefix.extend_from_slice(key);
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
    data: &'a [u8],
}

impl<'a> Block<'a> {
    pub fn len(&self) -> usize {
        self.data.len()
    }
}


#[cfg(test)]
mod tests {

    #[test]
    fn assert_always_true() {
        assert!(true)
    }
}
