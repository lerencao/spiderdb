extern crate bytes;
extern crate tempdir;
use self::bytes::BytesMut;
use std::io::Result;
use std::result::Result as StdResult;

use std::fs::{create_dir_all, read_dir, OpenOptions};
use failure::Error;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io::{ErrorKind, Result as IoResult, Seek, SeekFrom, Write};
use super::write::{Value, ValuePointer};

use super::segment::LogFile;

pub struct ValueOption {
    dir: String,
    segment_max_size: u32,
    sync: bool,
}

impl Default for ValueOption {
    fn default() -> Self {
        ValueOption {
            dir: tempdir::TempDir::new("valuelog")
                .unwrap()
                .path()
                .to_str()
                .unwrap()
                .to_string(),
            sync: false,
            segment_max_size: 1024 * 1024 * 128,
        }
    }
}

impl ValueOption {
    pub fn new(dir: &Path, segment_max_size: u32, sync: bool) -> ValueOption {
        ValueOption {
            dir: dir.to_str().unwrap().to_string(),
            segment_max_size,
            sync,
        }
    }
}

#[derive(Debug)]
pub struct ValueLog {
    dir_path: PathBuf,
    segment_max_size: u32,
    sync: bool,
    log_files: HashMap<u32, LogFile>,
    cur_fid: u32,
    write_buffer: Vec<u8>,
}

use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result as FmtResult;
impl Display for ValueLog {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "value_log(dir: {:?}, segment_max_size: {:?}, sync: {:?}, cur_fid: {:?})",
            &self.dir_path, self.segment_max_size, self.sync, self.cur_fid
        )
    }
}

// Init/open
impl ValueLog {
    const LOG_SUFFIX: &'static str = "vlog";
    fn get_value_log_dir_entry(path: &Path) -> Result<Vec<DirEntry>> {
        let entries: Vec<DirEntry> = read_dir(path)?
            .fold(Ok(vec![]), |acc, entry| {
                acc.and_then(|mut v| {
                    entry.map(|e| {
                        v.push(e);
                        v
                    })
                })
            })?
            .into_iter()
            .filter(|e| Self::is_log_file(e))
            .collect();
        Ok(entries)
    }

    pub fn open(opt: &ValueOption) -> StdResult<ValueLog, Error> {
        let dir_path = Path::new(&opt.dir).to_path_buf();
        // make sure the path exists.
        create_dir_all(&dir_path)?;
        // find all file paths belongs to value log.
        let entries = Self::get_value_log_dir_entry(&dir_path)?;

        // TODO: check all log ids are different.
        let mut log_ids: Vec<u32> = Vec::with_capacity(entries.len());
        for entry in entries.iter() {
            log_ids.push(Self::parse_fid(&entry.path())?);
        }
        log_ids.sort();

        // open the file of max id in read-write mode, others in readonly mode.
        // if no file exists, start with id of 0.
        let (prev_fids, max_fid) = match log_ids.split_last() {
            Some((&last, prev)) => (prev, Some(last)),
            None => (&log_ids[0..0], None),
        };

        // load prev files if any
        let mut log_files: HashMap<u32, LogFile> = HashMap::with_capacity(prev_fids.len() + 1);
        for &fid in prev_fids.iter() {
            let log_path = dir_path.join(&Self::fid_to_pathbuf(fid));
            let file = OpenOptions::new().read(true).open(&log_path)?;
            let log_file = LogFile::new(fid, &log_path, file, true)?;
            log_files.insert(log_file.fid(), log_file);
        }

        // load or create current log file
        let cur_fid = match max_fid {
            Some(fid) => fid,
            None => 0,
        };
        let log_path = dir_path.join(Self::fid_to_pathbuf(cur_fid));
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)?;

        let cur_log_file = LogFile::new(cur_fid, &log_path, file, false)?;

        log_files.insert(cur_log_file.fid(), cur_log_file);

        Ok(ValueLog {
            dir_path,
            segment_max_size: opt.segment_max_size,
            sync: opt.sync,
            cur_fid,
            log_files,
            write_buffer: Vec::with_capacity(1024 * 8),
        })
    }

    fn is_log_file(entry: &DirEntry) -> bool {
        let path = entry.path();
        path.is_file()
            && path.extension()
                .filter(|&s| s == ValueLog::LOG_SUFFIX)
                .is_some()
    }

    fn parse_fid(path: &Path) -> StdResult<u32, Error> {
        let base_name = path.file_stem().and_then(|n| n.to_str());
        assert!(base_name.is_some());
        let fid = base_name.unwrap().parse::<u32>()?;
        Ok(fid)
    }

    fn fid_to_pathbuf(fid: u32) -> PathBuf {
        PathBuf::from(format!("{:06}", fid)).with_extension(ValueLog::LOG_SUFFIX)
    }
}

// Impl write related operations on value log
impl ValueLog {
    pub fn segment_max_size(&self) -> u32 {
        self.segment_max_size
    }
    pub fn write_offset(&self) -> Option<u32> {
        self.active_segment().and_then(|s| s.write_offset())
    }

    pub fn active_segment_mut(&mut self) -> Option<&mut LogFile> {
        self.log_files.get_mut(&self.cur_fid)
    }
    pub fn active_segment(&self) -> Option<&LogFile> {
        self.log_files.get(&self.cur_fid)
    }

    pub fn write(&mut self, entries: &[Value]) -> IoResult<Vec<ValuePointer>> {
        self.rollover_if_necessary()?;
        // TODO: shrunk buffer ?
        self.write_buffer.clear();
        let mut value_pointers = Vec::with_capacity(entries.len());

        {
            let mut cur_offset: u32 = self.active_segment()
                .and_then(|s| s.write_offset())
                .unwrap();
            for entry in entries {
                let len = entry.encode(&mut self.write_buffer)?;
                value_pointers.push(ValuePointer::new(self.cur_fid, cur_offset, len));
                cur_offset += len;
            }
            self.write_buffer.flush()?;
        }

        // write all entries.
        self.internal_write()?;

        Ok(value_pointers)
    }

    fn internal_write(&mut self) -> IoResult<()> {
        let segment = self.log_files.get_mut(&self.cur_fid).unwrap();
        segment.write_bytes(&self.write_buffer, self.sync)?;
        self.write_buffer.clear();
        Ok(())
    }

    fn should_rollover(&self) -> bool {
        let cur_write_offset = self.active_segment().unwrap().write_offset().unwrap();
        cur_write_offset >= self.segment_max_size
    }
    fn rollover_if_necessary(&mut self) -> IoResult<()> {
        use std::mem::drop;
        if self.should_rollover() {
            let segment = self.log_files.remove(&self.cur_fid).unwrap();
            let fp = segment.file_path().to_path_buf();
            drop(segment);
            // reopen in readonly mode
            let file = OpenOptions::new().read(true).open(&fp)?;
            let segment = LogFile::new(self.cur_fid, &fp, file, true)?;
            self.log_files.insert(self.cur_fid, segment);

            self.cur_fid += 1;
            // create new segment
            let rollover_path = self.dir_path.join(Self::fid_to_pathbuf(self.cur_fid));
            let rollover_file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(&rollover_path)?;
            let rollover_segment =
                LogFile::new(self.cur_fid, &rollover_path, rollover_file, false)?;
            self.log_files.insert(self.cur_fid, rollover_segment);
        }

        Ok(())
    }
}

// Impl read related ops
impl ValueLog {
    pub fn read(&mut self, pointer: &ValuePointer) -> IoResult<Value> {
        if pointer.fid() == self.cur_fid && pointer.offset() >= self.write_offset().unwrap() {
            Err(ErrorKind::UnexpectedEof)?
        }
        match self.log_files.get_mut(&pointer.fid()) {
            Some(segment) => {
                let mut buf: &[u8] = &segment.read_bytes(pointer.offset(), pointer.len())?;
                Value::decode(&mut buf)
            }
            None => Err(ErrorKind::UnexpectedEof)?,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn test_open() {
        let tmp_dir = tempdir::TempDir::new("test_open").unwrap();
        let log1 = tmp_dir.path().join("000001.vlog");
        File::create(log1).unwrap();
        let log2 = tmp_dir.path().join("000002.vlog");
        File::create(log2).unwrap();

        let vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });

        assert!(vl.is_ok(), format!("{:?}", vl.err()));
        let vlog = vl.unwrap();
        assert_eq!(vlog.log_files.len(), 2);
    }

    #[test]
    fn test_open_with_invalid_log_file() {
        let tmp_dir = tempdir::TempDir::new("test_open_with_invalid_log_file").unwrap();
        let log1 = tmp_dir.path().join("000001.vlog");
        File::create(log1).unwrap();
        let log2 = tmp_dir.path().join("v1.vlog");
        File::create(log2).unwrap();

        let vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });

        assert!(vl.is_err())
    }

    #[test]
    fn test_fid_to_pathbuf() {
        assert_eq!(
            format!("{:06}.{}", 12, ValueLog::LOG_SUFFIX),
            ValueLog::fid_to_pathbuf(12).to_str().unwrap()
        );
    }

    #[test]
    fn test_pathbuf_to_fid() {
        for path in &["6.vlog", "06.vlog", "0006.vlog", "000006.vlog"] {
            let fid = ValueLog::parse_fid(&PathBuf::from(path));
            assert!(fid.is_ok());
            assert_eq!(fid.ok().unwrap(), 6);
        }
    }

    #[test]
    fn test_write_entries() {
        let tmp_dir = tempdir::TempDir::new("value_log").unwrap();
        let mut vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        }).unwrap();
        let ents = vec![Value::new(b"key1", b"value1")];
        let len = vl.write(&ents);
        assert!(len.is_ok());
    }

    #[test]
    fn test_write_rollover() {
        // max segment size set to 32, insert kv, with size 8, 8 + 4 + 4 + 4 = 20.
        let tmp_dir = tempdir::TempDir::new("value_log").unwrap();
        let mut vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
            segment_max_size: 32,
            ..Default::default()
        }).unwrap();

        let ents = vec![Value::new(b"11", b"222222"); 2];
        vl.write(&ents).unwrap();
        assert!(vl.should_rollover());
        assert_eq!(0, vl.active_segment().unwrap().fid());

        vl.write(&ents[0..1]).unwrap();
        assert_eq!(1, vl.active_segment().unwrap().fid());
        assert!(!vl.should_rollover());

        vl.write(&ents).unwrap();
        assert_eq!(1, vl.active_segment().unwrap().fid());
        assert!(vl.should_rollover());

        vl.write(&ents[0..1]).unwrap();
        assert_eq!(2, vl.active_segment().unwrap().fid());
        assert!(!vl.should_rollover());
    }

    #[test]
    fn test_read_and_write() {
        let tmp_dir = tempdir::TempDir::new("value_log").unwrap();
        let mut vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
            segment_max_size: 32,
            ..Default::default()
        }).unwrap();
        let ents = vec![Value::new(b"1", b"1"), Value::new(b"2", b"2")];
        let pointers = vl.write(&ents).unwrap();

        // read
        let value = vl.read(&pointers[0]);
        assert!(value.is_ok());
        assert_eq!(value.unwrap(), ents[0]);

        // write anothers
        let ents2 = vec![Value::new(b"3", b"3"), Value::new(b"4", b"4")];
        let pointers2 = vl.write(&ents2).unwrap();
        // then read
        let value = vl.read(&pointers[1]);
        assert!(value.is_ok());
        assert_eq!(value.unwrap(), ents[1]);

        // read anothers
        for i in 0..pointers2.len() {
            let value = vl.read(&pointers2[i]);
            assert_eq!(ents2[i], value.unwrap());
        }
    }

}

#[cfg(test)]
mod read_tests {
    use super::*;

    #[test]
    fn test_read_value() {
        let tmp_dir = tempdir::TempDir::new("value_log").unwrap();
        let mut vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
            segment_max_size: 32,
            ..Default::default()
        }).unwrap();
        let ents = vec![Value::new(b"11", b"222222"), Value::new(b"22", b"333333")];
        let pointers = vl.write(&ents).unwrap();
        for i in 0..pointers.len() {
            let value = vl.read(&pointers[i]);
            assert!(value.is_ok());
            assert_eq!(value.unwrap(), ents[i]);
        }
    }
}
