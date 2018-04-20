extern crate crc;
use self::crc::{Hasher32, crc32};
use std::io::Result as IoResult;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Value {
    key: Vec<u8>,
    value: Vec<u8>,
}
impl Value {
    pub fn new(key: &[u8], value: &[u8]) -> Value {
        Value {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }
}

struct ValueHeader {
    klen: u32,
    vlen: u32,
}

impl ValueHeader {
    pub fn encode<T: WriteBytesExt>(&self, writer: &mut T) -> IoResult<u32> {
        writer.write_u32::<BigEndian>(self.klen)?;
        writer.write_u32::<BigEndian>(self.vlen)?;
        Ok(4 + 4)
    }

    pub fn decode<T: ReadBytesExt>(reader: &mut T) -> IoResult<ValueHeader> {
        let klen = reader.read_u32::<BigEndian>()?;
        let vlen = reader.read_u32::<BigEndian>()?;
        Ok(ValueHeader { klen, vlen })
    }
}

impl Value {
    fn get_header(&self) -> ValueHeader {
        ValueHeader {
            klen: self.key.len() as u32,
            vlen: self.value.len() as u32,
        }
    }

    pub fn decode<T: ReadBytesExt>(reader: &mut T) -> IoResult<Value> {
        let header = ValueHeader::decode(reader)?;
        let mut key = Vec::with_capacity(header.klen as usize);
        key.resize(header.klen as usize, 0);
        let mut value = Vec::with_capacity(header.vlen as usize);
        value.resize(header.vlen as usize, 0);

        reader.read_exact(&mut key)?;
        reader.read_exact(&mut value)?;
        // TODO: check crc
        let _crc32 = reader.read_u32::<BigEndian>()?;

        Ok(Value { key, value })
    }

    pub fn encode<T: WriteBytesExt>(&self, writer: &mut T) -> IoResult<u32> {
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

#[derive(Copy, Clone, Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ValuePointer {
    fid: u32,
    offset: u32,
    len: u32,
}

impl ValuePointer {
    const SIZE: u32 = 12;

    pub fn new(fid: u32, offset: u32, len: u32) -> ValuePointer {
        ValuePointer { fid, offset, len }
    }
    #[inline]
    pub fn fid(&self) -> u32 {
        self.fid
    }
    #[inline]
    pub fn offset(&self) -> u32 {
        self.offset
    }
    #[inline]
    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn encode<T: WriteBytesExt>(&self, writer: &mut T) -> IoResult<u32> {
        writer.write_u32::<BigEndian>(self.fid)?;
        writer.write_u32::<BigEndian>(self.len)?;
        writer.write_u32::<BigEndian>(self.offset)?;
        writer.flush()?;
        Ok(ValuePointer::SIZE)
    }

    pub fn decode() {}
}

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    pub fn test_header_encode() {
        let h = ValueHeader {
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
        let entry = Value {
            key: vec![1, 2, 3, 4],
            value: vec![5, 6, 7, 8, 9, 10],
        };
        let mut buf = Vec::new();
        let len = entry.encode(&mut buf).unwrap();
        assert_eq!(8 + entry.key.len() + entry.value.len() + 4, len as usize);
        assert_eq!(
            vec![1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            &buf[8..(buf.len() - 4)]
        );
    }
}
