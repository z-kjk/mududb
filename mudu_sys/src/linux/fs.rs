use crate::api::fs::SysFs;
use mudu::common::result::RS;
use mudu::error::ec::EC;
use mudu::m_error;
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::os::unix::fs::{FileExt, OpenOptionsExt};
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};

pub struct LinuxFs;

impl SysFs for LinuxFs {
    fn open(&self, path: &Path, flags: i32, _mode: u32) -> RS<File> {
        let mut options = OpenOptions::new();
        let read = (flags & libc::O_RDWR) != 0 || (flags & libc::O_WRONLY) == 0;
        let write = (flags & libc::O_RDWR) != 0 || (flags & libc::O_WRONLY) != 0;
        options.read(read);
        options.write(write);
        options.create((flags & libc::O_CREAT) != 0);
        options.truncate((flags & libc::O_TRUNC) != 0);
        options.append((flags & libc::O_APPEND) != 0);
        #[cfg(unix)]
        {
            options.mode(_mode);
        }
        options
            .open(path)
            .map_err(|e| m_error!(EC::IOErr, "open file error", e))
    }

    fn read_exact_at(&self, file: &File, len: usize, offset: u64) -> RS<Vec<u8>> {
        let mut buf = vec![0u8; len];
        let mut read = 0usize;
        while read < len {
            #[cfg(unix)]
            let rc = file
                .read_at(&mut buf[read..], offset + read as u64)
                .map_err(|e| m_error!(EC::IOErr, "read file error", e))?;
            #[cfg(windows)]
            let rc = file
                .seek_read(&mut buf[read..], offset + read as u64)
                .map_err(|e| m_error!(EC::IOErr, "read file error", e))?;
            if rc == 0 {
                return Err(m_error!(EC::IOErr, "unexpected EOF while reading file"));
            }
            read += rc;
        }
        Ok(buf)
    }

    fn write_all_at(&self, file: &File, payload: &[u8], offset: u64) -> RS<()> {
        let mut written = 0usize;
        while written < payload.len() {
            #[cfg(unix)]
            let rc = file
                .write_at(&payload[written..], offset + written as u64)
                .map_err(|e| m_error!(EC::IOErr, "write file error", e))?;
            #[cfg(windows)]
            let rc = file
                .seek_write(&payload[written..], offset + written as u64)
                .map_err(|e| m_error!(EC::IOErr, "write file error", e))?;
            if rc == 0 {
                return Err(m_error!(EC::IOErr, "write file returned zero bytes"));
            }
            written += rc;
        }
        Ok(())
    }

    fn fsync(&self, file: &File) -> RS<()> {
        file.sync_all()
            .map_err(|e| m_error!(EC::IOErr, "flush file error", e))
    }

    fn close(&self, file: File) -> RS<()> {
        drop(file);
        Ok(())
    }

    fn create_dir_all(&self, path: &Path) -> RS<()> {
        std::fs::create_dir_all(path).map_err(|e| m_error!(EC::IOErr, "create directory error", e))
    }

    fn read_dir(&self, path: &Path) -> RS<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in
            std::fs::read_dir(path).map_err(|e| m_error!(EC::IOErr, "read directory error", e))?
        {
            let entry = entry.map_err(|e| m_error!(EC::IOErr, "read directory entry error", e))?;
            paths.push(entry.path());
        }
        Ok(paths)
    }

    fn metadata_len(&self, path: &Path) -> RS<u64> {
        std::fs::metadata(path)
            .map_err(|e| m_error!(EC::IOErr, "read file metadata error", e))
            .map(|metadata| metadata.len())
    }

    fn read_all(&self, path: &Path) -> RS<Vec<u8>> {
        std::fs::read(path).map_err(|e| m_error!(EC::IOErr, "read file error", e))
    }

    fn remove_file_if_exists(&self, path: &Path) -> RS<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(m_error!(EC::IOErr, "remove file error", err)),
        }
    }
}
