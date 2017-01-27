use std::fs::File;
use std::io::BufReader;
use file::metadata::{RowGroupMetaData, FileMetaData};

pub trait ParquetFileInfo {
  /// Get the metadata about this file
  fn metadata(&self) -> FileMetaData;

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

static FOOTER_SIZE: u32 = 8;

impl ParquetFileInfo for ParquetFileReader {
  fn metadata(&self) -> FileMetaData {
    let mut file_metadata = FileMetaData{};
    if let Ok(file_info) = self.buf.get_ref().metadata() {
      println!("File size: {}", file_info.len());
    }
    file_metadata
  }

  fn get_row_group(&self, i: usize) -> Box<ParquetRowGroupInfo> {
    unimplemented!()
  }
}

