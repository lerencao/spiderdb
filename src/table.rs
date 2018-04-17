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
    blocks: Vec<Block>, // TODO: self referential struct
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

    fn read_block(mmap: &Mmap) -> io::Result<Vec<Block>> {
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
        let mut blocks = Vec::with_capacity(restart_len);
        for i in 0..restart_len {
            let off = offsets_buf.read_u32::<BigEndian>()?;
            block_index[i] = KeyOffset {
                offset: prev as usize,
                len: (off - prev) as usize,
            };
            blocks[i] = Block {
                offset: prev as u32,
                data: Table::read_mmap(mmap, prev as usize, (off - prev) as usize)?
            };
            prev = off;
        }

        Ok(blocks)
    }

    fn read_mmap(mmap: &[u8], offset: usize, size: usize) -> io::Result<&[u8]> {
        if mmap.len() < offset + size {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        } else {
            return Ok(&mmap[offset..offset + size]);
        }
    }
}

//impl<'a> Index<usize> for &'a Table {
//    type Output = io::Result<Block<'a>>;
//
//    fn index(&self, index: usize) -> &Self::Output {
//        let bi = &self.block_index[index];
//        let data = Table::read_mmap(&self.mmap, bi.offset, bi.len)
//            .map(|da| Block{
//            offset: bi.offset as u32,
//            data: da,
//        });
//        data
//    }
//}

pub struct Block<'a> {
    offset: u32,
    data: &'a [u8],
}

struct Header {
    plen: u16, // Overlap with base key.
    klen: u16, // Length of the diff.
    vlen: u16, // Length of value.
    prev: u32, // Offset for the previous key-value pair. The offset is relative to block base offset.
}

impl Header {
    pub const SIZE: u16 = 16;
}
