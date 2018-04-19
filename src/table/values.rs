extern crate tempdir;

use std::io::Write;
use std::io::Result;
use std::result::Result as StdResult;
use byteorder::WriteBytesExt;
use byteorder::BigEndian;

use std::io;
use std::io::Seek;
use std::fs;
use std::path;
use failure::Error;
use std::fs::DirEntry;
use std::path::Path;
use std::path::PathBuf;
use std::collections::HashMap;
use std::fs::File;
use std::io::SeekFrom;

#[derive(Copy, Clone, Default, Debug, Ord, PartialOrd, Eq, PartialEq)]
struct ValuePointer {
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

struct LogFile {
    fid: u32,
    file_path: PathBuf,
    file: fs::File,
}

impl LogFile {
    pub fn new(fid: u32, file_path: &Path, file: File) -> LogFile {
        LogFile {
            fid,
            file_path: file_path.to_path_buf(),
            file,
        }
    }
}

pub struct ValueLog {
    dir_path: PathBuf,
    log_files: HashMap<u32, LogFile>,
    cur_fid: u32,
    cur_write_offset: u64,
}

pub struct ValueOption {
    dir: String,
}

impl ValueLog {
    const LOG_SUFFIX: &'static str = "vlog";
    fn get_value_log_dir_entry(path: &Path) -> Result<Vec<DirEntry>> {
        let entries: Vec<DirEntry> = fs::read_dir(path)?
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
        let dir_path = path::Path::new(&opt.dir).to_path_buf();
        // make sure the path exists.
        fs::create_dir_all(&dir_path)?;
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
            let file = fs::OpenOptions::new().read(true).open(&log_path)?;
            let log_file = LogFile::new(fid, &log_path, file);
            log_files.insert(log_file.fid, log_file);
        };

        // load or create current log file
        let cur_fid = match max_fid {
            Some(fid) => fid,
            None => 0
        };
        let log_path = dir_path.join(Self::fid_to_pathbuf(cur_fid));
        let mut file = fs::OpenOptions::new()
            .create(true).read(true).append(true)
            .open(&log_path)?;
        let last_offset = file.seek(SeekFrom::End(0))?;

        let cur_log_file = LogFile::new(cur_fid, &log_path, file);

        log_files.insert(cur_log_file.fid, cur_log_file);

        Ok(ValueLog {
            dir_path,
            cur_fid,
            cur_write_offset: last_offset,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assert_always_true() {
        assert!(true)
    }

    #[test]
    fn test_open() {
        let tmp_dir = tempdir::TempDir::new("test_open").unwrap();
        let log1 = tmp_dir.path().join("000001.vlog");
        fs::File::create(log1).unwrap();
        let log2 = tmp_dir.path().join("000002.vlog");
        fs::File::create(log2).unwrap();

        let vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
        });

        assert!(vl.is_ok(), format!("{:?}", vl.err()));
        let vlog = vl.unwrap();
        assert_eq!(vlog.log_files.len(), 2);
    }

    #[test]
    fn test_open_with_invalid_log_file() {
        let tmp_dir = tempdir::TempDir::new("test_open_with_invalid_log_file").unwrap();
        let log1 = tmp_dir.path().join("000001.vlog");
        fs::File::create(log1).unwrap();
        let log2 = tmp_dir.path().join("v1.vlog");
        fs::File::create(log2).unwrap();

        let vl = ValueLog::open(&ValueOption {
            dir: tmp_dir.path().to_str().unwrap().to_string(),
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
}
