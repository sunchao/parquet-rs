// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fs::File;
use std::io::{Read, BufReader, Seek, SeekFrom};
use std::rc::Rc;
use std::cell::RefCell;

use errors::{Result, ParquetError};
use file::metadata::{RowGroupMetaData, FileMetaData, ParquetMetaData};
use byteorder::{LittleEndian, ByteOrder};
use thrift::transport::{TTransport, TBufferTransport};
use thrift::protocol::TCompactInputProtocol;
use parquet_thrift::parquet::FileMetaData as TFileMetaData;
use schema::types;

pub trait ParquetFileReader {
  /// Get the metadata about this file
  fn metadata(&mut self) -> Result<ParquetMetaData>;

  /// Get the `i`th row group reader. Note this doesn't do bound check.
  fn get_row_group(&self, _: usize) -> Box<ParquetRowGroupReader>;
}

/// TODO: add page reader
pub trait ParquetRowGroupReader {
  fn metadata(&self) -> RowGroupMetaData;
}

pub struct SerializedParquetFileReader {
  buf: BufReader<File>
}

impl SerializedParquetFileReader {
  pub fn new(b: BufReader<File>) -> Self {
    Self { buf: b }
  }
}

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

impl ParquetFileReader for SerializedParquetFileReader {
  fn metadata(&mut self) -> Result<ParquetMetaData> {
    let file_size =
      match self.buf.get_ref().metadata() {
        Ok(file_info) => file_info.len(),
        Err(e) => return Err(io_err!(e, "Fail to get metadata for file"))
      };
    if file_size < (FOOTER_SIZE as u64) {
      return Err(parse_err!("Corrputed file, smaller than file footer"));
    }
    let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
    self.buf.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    self.buf.read_exact(&mut footer_buffer)?;
    if footer_buffer[4..] != PARQUET_MAGIC {
      return Err(parse_err!("Invalid parquet file. Corrupt footer."));
    }
    let metadata_len = LittleEndian::read_i32(&footer_buffer[0..4]);
    if metadata_len < 0 {
      return Err(parse_err!(
        "Invalid parquet file. Metadata length is less than zero ({})",
        metadata_len));
    }
    let mut metadata_buffer = vec![0; metadata_len as usize];
    let metadata_start: i64 = file_size as i64 - FOOTER_SIZE as i64 - metadata_len as i64;
    if metadata_start < 0 {
      return Err(parse_err!(
        "Invalid parquet file. Metadata start is less than zero ({})",
        metadata_start))
    }
    self.buf.seek(SeekFrom::Start(metadata_start as u64))?;
    self.buf.read_exact(metadata_buffer.as_mut_slice())
      .map_err(|e| io_err!(e, "Failed to read metadata"))?;

    // TODO: do row group filtering
    let mut transport = TBufferTransport::with_capacity(metadata_len as usize, 0);
    transport.set_readable_bytes(metadata_buffer.as_mut_slice());
    let transport = Rc::new(RefCell::new(Box::new(transport) as Box<TTransport>));
    let mut prot = TCompactInputProtocol::new(transport);
    let mut t_file_metadata: TFileMetaData = TFileMetaData::read_from_in_protocol(&mut prot)
      .map_err(|e| thrift_err!(e, "Could not parse metadata"))?;
    let schema: Box<types::Type> = types::from_thrift(&mut t_file_metadata.schema)?;
    let mut row_groups = Vec::new();
    for rg in t_file_metadata.row_groups {
      row_groups.push(RowGroupMetaData::from_thrift(rg)?);
    }

    let file_metadata = FileMetaData::new(
      t_file_metadata.version,
      t_file_metadata.num_rows,
      t_file_metadata.created_by,
      schema);
    Ok(ParquetMetaData::new(file_metadata, row_groups))
  }

  fn get_row_group(&self, _: usize) -> Box<ParquetRowGroupReader> {
    unimplemented!()
  }
}
