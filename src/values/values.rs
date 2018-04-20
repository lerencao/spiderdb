extern crate bytes;
extern crate tempdir;
use self::bytes::BytesMut;
use std::io::Result;
use std::result::Result as StdResult;
use byteorder::WriteBytesExt;
use byteorder::BigEndian;

use std::fs::{create_dir_all, read_dir, OpenOptions};
use failure::Error;
use std::fs::DirEntry;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::io::{Result as IoResult, Seek, SeekFrom, Write};
use super::write::Entry;

#[derive(Copy, Clone, Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct ValuePointer {
    fid: u32,
    offset: u32,
    len: u32,
}

impl ValuePointer {
    const Size: u32 = 12;

    pub fn encode<T: WriteBytesExt>(&self, writer: &mut T) -> Result<u32> {
        writer.write_u32::<BigEndian>(self.fid)?;
        writer.write_u32::<BigEndian>(self.len)?;
        writer.write_u32::<BigEndian>(self.offset)?;
        writer.flush()?;
        Ok(ValuePointer::Size)
    }

    pub fn decode() {}
}

use super::segment::LogFile;

pub struct ValueLog {
    dir_path: PathBuf,
    segment_max_size: u32,
    log_files: HashMap<u32, LogFile>,
    cur_fid: u32,
}

pub struct ValueOption {
    dir: String,
    segment_max_size: u32,
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
            segment_max_size: 1024 * 1024 * 128,
        }
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
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&log_path)?;

        let cur_log_file = LogFile::new(cur_fid, &log_path, file, false)?;

        log_files.insert(cur_log_file.fid(), cur_log_file);

        Ok(ValueLog {
            dir_path,
            segment_max_size: opt.segment_max_size,
            cur_fid,
            log_files,
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
    pub fn active_segment_mut(&mut self) -> Option<&mut LogFile> {
        self.log_files.get_mut(&self.cur_fid)
    }
    pub fn active_segment(&self) -> Option<&LogFile> {
        self.log_files.get(&self.cur_fid)
    }

    pub fn write<'a>(&mut self, entries: &[Entry]) -> IoResult<Vec<ValuePointer>> {
        self.rollover_if_necessary()?;
        use self::bytes::BufMut;

        let mut buffer_writer = BytesMut::with_capacity(8 * 1024).writer();
        let mut value_pointers = Vec::with_capacity(entries.len());
        let mut cur_offset: u32 = self.active_segment().and_then( |s| s.write_offset()).unwrap();
        for entry in entries {
            let len = entry.encode(&mut buffer_writer)?;
            value_pointers.push(ValuePointer {
                fid: self.cur_fid,
                offset: cur_offset,
                len: len
            });
            cur_offset += len;
        }

        // flush after all entries written.
        {
            buffer_writer.flush()?;
            let segment = self.active_segment_mut().unwrap();
            segment.write_all(buffer_writer.get_ref())?;
            segment.flush()?;
        }

        let len = buffer_writer.get_ref().len() as u64;
        buffer_writer.get_mut().clear();
        Ok(value_pointers)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    #[test]
    fn assert_always_true() {
        assert!(true)
    }

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
        let ents = vec![Entry::new(b"key1", b"value1")];
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
        }).unwrap();

        let ents = vec![Entry::new(b"11", b"222222"); 2];
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
}
