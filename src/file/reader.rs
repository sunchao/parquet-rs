use std::fs::File;
use std::io::{Read, BufReader, Seek, SeekFrom};
use std::rc::Rc;
use std::cell::RefCell;

use errors::{Result, ParquetError};
use file::metadata::{RowGroupMetaData, FileMetaData};
use byteorder::{LittleEndian, ByteOrder};
use thrift::transport::{TTransport, TBufferTransport};
use thrift::protocol::TCompactInputProtocol;
use parquet_thrift::parquet::FileMetaData as TFileMetaData;

pub trait ParquetFileInfo {
  /// Get the metadata about this file
  fn metadata(&mut self) -> Result<FileMetaData>;

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
  fn metadata(&mut self) -> Result<FileMetaData> {
    let file_metadata = FileMetaData{};
    let file_size =
      if let Ok(file_info) = self.buf.get_ref().metadata() {
        file_info.len() as usize
      } else {
        return err!("Fail to get metadata for file");
      };
    if file_size < FOOTER_SIZE {
      return err!("Corrputed file, smaller than file footer");
    }
    let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
    self.buf.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    self.buf.read_exact(&mut footer_buffer)?;
    if footer_buffer[4..] != PARQUET_MAGIC {
      return err!("Invalid parquet file. Corrupt footer.");
    }
    let metadata_len = LittleEndian::read_i32(&footer_buffer[0..4]) as usize;
    let mut metadata_buffer = vec![0; metadata_len];
    let metadata_start = file_size - FOOTER_SIZE - metadata_len;
    if metadata_start < 0 {
      return err!("Invalid parquet file. Metadata start is less than zero ({})", metadata_start)
    }
    self.buf.seek(SeekFrom::Start(metadata_start as u64))?;
    match self.buf.read_exact(metadata_buffer.as_mut_slice()) {
      Ok(_) => (),
      Err(e) => {
        return err!("Failed to read metadata {}", e);
      }
    }

    // TODO: do row group filtering
    let mut transport = TBufferTransport::with_capacity(metadata_len, 0);
    transport.set_readable_bytes(metadata_buffer.as_mut_slice());
    let transport = Rc::new(RefCell::new(Box::new(transport) as Box<TTransport>));
    let mut prot = TCompactInputProtocol::new(transport);
    let t_file_metadata = match TFileMetaData::read_from_in_protocol(&mut prot) {
      Ok(fm) => fm,
      Err(e) => {
        return err!("Could not parse metadata, error: {}", e);
      }
    };

    // TODO: convert from t_metadata
    Ok(file_metadata)
  }

  fn get_row_group(&self, i: usize) -> Box<ParquetRowGroupInfo> {
    unimplemented!()
  }
}
