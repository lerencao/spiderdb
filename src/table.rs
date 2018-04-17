extern crate byteorder;
extern crate memmap;
use std::fs;
use std::fs::File;
use std::fs::Metadata;
use std::io;
use std::os;
use std::io::Read;
use self::memmap::Mmap;
use std::io::SeekFrom;
use self::byteorder::ReadBytesExt;
use self::byteorder::BigEndian;
use std::io::Cursor;
use std::ops::Index;

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

    pub fn size(&self) -> u64 { self.table_size }
    pub fn id(&self) -> u64 { self.id }

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

#[derive(Default)]
struct Header {
    plen: u16, // Overlap with base key.
    klen: u16, // Length of the diff.
    vlen: u16, // Length of value.
    // Offset for the previous key-value pair. The offset is relative to block base offset.
    prev: u32,
}

impl Header {
    pub const SIZE: u16 = 16;
}


pub struct Block<'a> {
    offset: u32,
    data: &'a [u8],
}

pub struct BlockIterator<'a, 'b:'a> {
    block: &'a Block<'b>,
    pos: u32,
    base_key: Vec<u8>,
    key: Vec<u8>,
    val: Vec<u8>,
    init: bool,
    last: Header,
}

impl <'a, 'b: 'a> From<&'a Block<'b>> for BlockIterator<'a, 'b> {
    fn from(block: &'a Block<'b>) -> Self {
        BlockIterator {
            block: block,
            pos: 0,
            base_key: vec![],
            key: vec![],
            val: vec![],
            init: false,
            last: Header::default(),
        }
    }

}

impl <'a, 'b: 'a> BlockIterator<'a, 'b> {
    pub fn reset(&mut self)  {
        self.pos = 0;
        self.base_key = vec![];
        self.key = vec![];
        self.val = vec![];
        self.init = false;
        self.last = Header::default();
    }
}


impl <'a, 'b: 'a> Iterator for BlockIterator<'a, 'b> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: implement it
        unimplemented!()
    }
}
