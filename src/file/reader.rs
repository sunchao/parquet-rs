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
use column::page::PageReader;

// ----------------------------------------------------------------------
// APIs for file & row group readers

/// Parquet file reader API. With this, user can get metadata information
/// about the Parquet file, and can get reader for each row group.
pub trait FileReader {
  /// Get metadata information about this file
  fn metadata(&self) -> &ParquetMetaData;

  /// Get the `i`th row group reader. Note this doesn't do bound check.
  /// N.B.: Box<..> has 'static lifetime in default, but here we need
  /// the lifetime to be the same as this. Otherwise, the row group metadata
  /// stored in the row group reader may outlive the file reader.
  fn get_row_group<'a>(&'a self, i: usize) -> Box<RowGroupReader + 'a>;
}

/// Parquet row group reader API. With this, user can get metadata
/// information about the row group, as well as readers for each individual
/// column chunk
/// The lifetime 'a is for the metadata inherited from the parent file reader
pub trait RowGroupReader<'a> {
  /// Get metadata information about this row group.
  /// The result metadata is owned by the parent file reader.
  fn metadata(&self) -> &'a RowGroupMetaData;

  /// Get page reader for the `i`th column chunk
  fn get_page_reader(&self, i: usize) -> Box<PageReader>;
}


// ----------------------------------------------------------------------
// Serialized impl for file & row group readers

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

pub struct SerializedFileReader<'a> {
  file: &'a File,
  buf: BufReader<&'a File>,
  metadata: ParquetMetaData
}

impl<'a> SerializedFileReader<'a> {
  pub fn new(file: &'a File) -> Result<Self> {
    let mut buf = BufReader::new(file);
    let metadata = Self::parse_metadata(&mut buf)?;
    Ok(Self { file: file, buf: buf, metadata: metadata })
  }

  fn parse_metadata(buf: &mut BufReader<&'a File>) -> Result<ParquetMetaData> {
    let file_size =
      match buf.get_ref().metadata() {
        Ok(file_info) => file_info.len(),
        Err(e) => return Err(io_err!(e, "Fail to get metadata for file"))
      };
    if file_size < (FOOTER_SIZE as u64) {
      return Err(parse_err!("Corrputed file, smaller than file footer"));
    }
    let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
    buf.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    buf.read_exact(&mut footer_buffer)?;
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
    buf.seek(SeekFrom::Start(metadata_start as u64))?;
    buf.read_exact(metadata_buffer.as_mut_slice())
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
}

impl<'b> FileReader for SerializedFileReader<'b> {
  fn metadata(&self) -> &ParquetMetaData {
    &self.metadata
  }

  fn get_row_group<'a>(&'a self, i: usize) -> Box<RowGroupReader + 'a> {
    let row_group_metadata = self.metadata.row_group(i);
    let row_group_reader = SerializedRowGroupReader::new(self.file, row_group_metadata);
    Box::new(row_group_reader)
  }
}

/// A serialized impl for row group reader
/// Here 'a is the lifetime for the incoming file reader, 'b is
/// the lifetime for the parent file reader
pub struct SerializedRowGroupReader<'a, 'b> {
  file: &'a File,
  buf: BufReader<&'a File>,
  metadata: &'b RowGroupMetaData
}

impl<'a, 'b> SerializedRowGroupReader<'a, 'b> {
  pub fn new(file: &'a File, metadata: &'b RowGroupMetaData) -> Self {
    let buf = BufReader::new(file);
    Self { file: file, buf: buf, metadata: metadata }
  }
}

impl<'a, 'b> RowGroupReader<'b> for SerializedRowGroupReader<'a, 'b> {
  fn metadata(&self) -> &'b RowGroupMetaData {
    self.metadata
  }

  fn get_page_reader(&self, i: usize) -> Box<PageReader> {
    unimplemented!()
  }
}
