use mudu::common::result::RS;
use std::fs::File;
use std::path::{Path, PathBuf};

pub trait SysFs: Send + Sync {
    fn open(&self, path: &Path, flags: i32, mode: u32) -> RS<File>;
    fn read_exact_at(&self, file: &File, len: usize, offset: u64) -> RS<Vec<u8>>;
    fn write_all_at(&self, file: &File, payload: &[u8], offset: u64) -> RS<()>;
    fn fsync(&self, file: &File) -> RS<()>;
    fn close(&self, file: File) -> RS<()>;

    fn create_dir_all(&self, path: &Path) -> RS<()>;
    fn read_dir(&self, path: &Path) -> RS<Vec<PathBuf>>;
    fn metadata_len(&self, path: &Path) -> RS<u64>;
    fn read_all(&self, path: &Path) -> RS<Vec<u8>>;
    fn remove_file_if_exists(&self, path: &Path) -> RS<()>;
}
