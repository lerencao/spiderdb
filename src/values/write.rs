extern crate crc;
use self::crc::{crc32, Hasher32};
use std::io::{Result as IoResult};
use byteorder::{BigEndian, WriteBytesExt};
#[derive(Clone)]
pub struct Entry {
    key: Vec<u8>,
    value: Vec<u8>,
}
impl Entry {
    pub fn new(key: &[u8], value: &[u8]) -> Entry {
        Entry {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }
}

struct EntryHeader {
    klen: u32,
    vlen: u32,
}

impl EntryHeader {
    pub fn encode<T: WriteBytesExt>(&self, writer: &mut T) -> IoResult<u32> {
        writer.write_u32::<BigEndian>(self.klen)?;
        writer.write_u32::<BigEndian>(self.vlen)?;
        Ok(4 + 4)
    }
}

impl Entry {
    fn get_header(&self) -> EntryHeader {
        EntryHeader {
            klen: self.key.len() as u32,
            vlen: self.value.len() as u32,
        }
    }

    pub fn encode<T: WriteBytesExt>(&self, writer: &mut T) ->  IoResult<u32> {
        let header = self.get_header();
        let mut digest = crc32::Digest::new(crc32::CASTAGNOLI);

        let mut buf = Vec::with_capacity(8);
        let header_size = header.encode(&mut buf)?;
        writer.write_all(&buf)?;
        digest.write(&buf);

        writer.write_all(&self.key)?;
        digest.write(&self.key);
        writer.write_all(&self.value)?;
        digest.write(&self.value);

        let crc = digest.sum32();
        writer.write_u32::<BigEndian>(crc)?;

        Ok(header_size + header.klen + header.vlen + 4)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn test_header_encode() {
        let h = EntryHeader {
            klen: 255 + 256,
            vlen: 255 + 256 + 256 * 256,
        };
        let mut buf = Vec::new();
        let len = h.encode(&mut buf).unwrap();
        assert_eq!(8, len);
        assert_eq!(vec![0u8, 0, 1, 255, 0, 1, 1, 255], buf);
    }

    #[test]
    pub fn test_entry_encode() {
        let entry = Entry {
            key: vec![1,2,3,4],
            value: vec![5,6,7,8,9,10]
        };
        let mut buf = Vec::new();
        let len = entry.encode(&mut buf).unwrap();
        assert_eq!(8 + entry.key.len() + entry.value.len() + 4, len as usize);
        assert_eq!(vec![1u8,2,3,4,5,6,7,8,9,10], &buf[8..(buf.len() - 4)]);
    }
}
