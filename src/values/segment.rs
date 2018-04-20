use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{Result, Seek, SeekFrom};
use std::io::{Read, Result as IoResult, Write};

#[derive(Debug)]
pub struct LogFile {
    fid: u32,
    file_path: PathBuf,
    file: File,
    readonly: bool,
    write_offset: u32,
}

impl LogFile {
    pub fn new(fid: u32, file_path: &Path, file: File, readonly: bool) -> Result<LogFile> {
        let mut f = LogFile {
            fid,
            file_path: file_path.to_path_buf(),
            file,
            readonly,
            write_offset: 0,
        };
        if !readonly {
            // TODO: make sure that the file is not exceed 4GB, or else the u64 -> u32 will cause error.
            f.write_offset = f.file.seek(SeekFrom::End(0))? as u32;
        }
        Ok(f)
    }

    #[inline]
    pub fn fid(&self) -> u32 {
        self.fid
    }

    #[inline]
    pub fn file_path(&self) -> &Path {
        &self.file_path
    }

    // current write offset
    #[inline]
    pub fn write_offset(&self) -> Option<u32> {
        if self.readonly {
            None
        } else {
            Some(self.write_offset)
        }
    }
}

impl LogFile {
    pub fn read_bytes(&mut self, offset: u32, len: u32) -> IoResult<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset as u64))?;
        let mut buf = Vec::with_capacity(len as usize);
        buf.resize(len as usize, 0);
        self.file.read_exact(&mut buf)?;
        Ok(buf)
    }

    pub fn write_bytes(&mut self, buf: &[u8], sync: bool) -> IoResult<()> {
        self.file.seek(SeekFrom::Start(self.write_offset as u64))?;
        self.file.write_all(buf)?;
        self.write_offset += buf.len() as u32;

        if sync {
            self.file.flush()?;
            self.file.sync_data()?;
        }
        Ok(())
    }
}
