use std::path::{Path, PathBuf};
use std::fs::File;
use std::io::{Seek, SeekFrom, Result};
pub struct LogFile {
    fid: u32,
    file_path: PathBuf,
    file: File,
    readonly: bool,
    write_offset: u64,
}

impl LogFile {
    pub fn new(fid: u32, file_path: &Path, file: File, readonly: bool) -> Result<LogFile> {
        let mut f = LogFile {
            fid,
            file_path: file_path.to_path_buf(),
            file,
            readonly,
            write_offset: 0
        };
        if !readonly {
            f.write_offset = f.file.seek(SeekFrom::End(0))?;
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
    pub fn write_offset(&self) -> Option<u64> {
        if self.readonly {
            None
        } else {
            Some(self.write_offset)
        }
    }

//    pub fn reopen(&mut self, readonly: bool) -> IoResult<()> {
//        use std::mem::drop;
//        let former = &self.file;
//        drop(former);
////        if readonly {
////            self.readonly = true;
////            self.write_offset = 0;
////        }
//        Ok(())
//    }
}

use std::io::{Write, Result as IoResult};
impl Write for LogFile {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let write_size = self.file.write(buf)?;
        self.write_offset += write_size as u64;
        Ok(write_size)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.file.flush()?;
        self.file.sync_data()
    }
}
