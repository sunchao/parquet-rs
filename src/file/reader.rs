use std::fs::File;
use std::io::{Read, BufReader, Seek, SeekFrom};
use file::metadata::{RowGroupMetaData, FileMetaData};

pub trait ParquetFileInfo {
  /// Get the metadata about this file
  fn metadata(&mut self) -> FileMetaData;

  /// Get the `i`th row group. Note this doesn't do bound check.
  fn get_row_group(&self, i: usize) -> Box<ParquetRowGroupInfo>;
}

/// TODO: add page reader
pub trait ParquetRowGroupInfo {
  fn metadata(&self) -> RowGroupMetaData;
}

pub struct ParquetFileReader {
  buf: BufReader<File>
}

impl ParquetFileReader {
  pub fn new(b: BufReader<File>) -> Self {
    ParquetFileReader{buf: b}
  }
}

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = ['P' as u8, 'A' as u8, 'R' as u8, '1' as u8];

impl ParquetFileInfo for ParquetFileReader {
  fn metadata(&mut self) -> FileMetaData {
    let mut file_metadata = FileMetaData{};
    let file_size =
      if let Ok(file_info) = self.buf.get_ref().metadata() {
        file_info.len()
      } else {
        panic!("Fail to get metadata for file");
      };
    if file_size < FOOTER_SIZE as u64 {
      panic!("Corrputed file, smaller than file footer");
    }
    let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
    self.buf.seek(SeekFrom::End(-(FOOTER_SIZE as i64)));
    self.buf.read_exact(&mut footer_buffer);
    if footer_buffer[4..] != PARQUET_MAGIC {
      panic!("Invalid parquet file. Corrupt footer.");
    }
    file_metadata
  }

  fn get_row_group(&self, i: usize) -> Box<ParquetRowGroupInfo> {
    unimplemented!()
  }
}
