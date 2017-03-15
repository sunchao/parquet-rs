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
use std::io::{self, Read, BufReader, Seek, SeekFrom};
use std::rc::Rc;
use std::cell::RefCell;

use basic::{Compression, Encoding};
use errors::{Result, ParquetError};
use file::metadata::{RowGroupMetaData, FileMetaData, ParquetMetaData};
use byteorder::{LittleEndian, ByteOrder};
use thrift::transport::{TTransport, TBufferTransport};
use thrift::protocol::TCompactInputProtocol;
use parquet_thrift::parquet::FileMetaData as TFileMetaData;
use parquet_thrift::parquet::{PageType, PageHeader};
use schema::types;
use column::page::{Page, PageReader};
use compression::{Codec, create_codec};
use util::memory::{Buffer, MutableBuffer, ByteBuffer};

// ----------------------------------------------------------------------
// APIs for file & row group readers

/// Parquet file reader API. With this, user can get metadata information
/// about the Parquet file, and can get reader for each row group.
pub trait FileReader {
  /// Get metadata information about this file
  fn metadata(&self) -> &ParquetMetaData;

  /// Get the total number of row groups for this file
  fn num_row_groups(&self) -> usize;

  /// Get the `i`th row group reader. Note this doesn't do bound check.
  /// N.B.: Box<..> has 'static lifetime in default, but here we need
  /// the lifetime to be the same as this. Otherwise, the row group metadata
  /// stored in the row group reader may outlive the file reader.
  fn get_row_group<'a>(&'a self, i: usize) -> Result<Box<RowGroupReader + 'a>>;
}

/// Parquet row group reader API. With this, user can get metadata
/// information about the row group, as well as readers for each individual
/// column chunk
/// The lifetime 'a is for the metadata inherited from the parent file reader
pub trait RowGroupReader<'a> {
  /// Get metadata information about this row group.
  /// The result metadata is owned by the parent file reader.
  fn metadata(&self) -> &'a RowGroupMetaData;

  /// Get the total number of column chunks in this row group
  fn num_columns(&self) -> usize;

  /// Get page reader for the `i`th column chunk
  fn get_page_reader<'b>(&'b self, i: usize) -> Result<Box<PageReader + 'b>>;
}


/// A thin wrapper on `BufReader` to be used by Thrift transport
pub struct TMemoryBuffer<'a, T> where T: 'a + Read {
  buf_reader: &'a mut BufReader<T>,
  offset: usize
}

impl<'a, T: 'a + Read> TMemoryBuffer<'a, T> {
  pub fn new(buf_reader: &'a mut BufReader<T>) -> Self {
    Self { buf_reader: buf_reader, offset: 0 }
  }
}

impl<'a, T: 'a + Read> Read for TMemoryBuffer<'a, T> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let bytes_read = self.buf_reader.read(buf)?;
    self.offset += bytes_read;
    Ok(bytes_read)

  }
}

impl<'a, T: 'a + Read> io::Write for TMemoryBuffer<'a, T> {
  fn write(&mut self, _: &[u8]) -> io::Result<usize> {
    panic!("Not implemented")
  }

  fn flush(&mut self) -> io::Result<()> {
    panic!("Not implemented")
  }
}

// ----------------------------------------------------------------------
// Serialized impl for file & row group readers

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

pub struct SerializedFileReader {
  buf: BufReader<File>,
  metadata: ParquetMetaData
}

impl SerializedFileReader {
  pub fn new(file: File) -> Result<Self> {
    let mut buf = BufReader::new(file);
    let metadata = Self::parse_metadata(&mut buf)?;
    Ok(Self { buf: buf, metadata: metadata })
  }

  fn parse_metadata(buf: &mut BufReader<File>) -> Result<ParquetMetaData> {
    let file_metadata = buf.get_ref().metadata()?;
    let file_size = file_metadata.len();
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

impl FileReader for SerializedFileReader {
  fn metadata(&self) -> &ParquetMetaData {
    &self.metadata
  }

  fn num_row_groups(&self) -> usize {
    self.metadata.num_row_groups()
  }

  fn get_row_group<'a>(&'a self, i: usize) -> Result<Box<RowGroupReader + 'a>> {
    let row_group_metadata = self.metadata.row_group(i);
    let f = self.buf.get_ref().try_clone()?;
    Ok(Box::new(SerializedRowGroupReader::new(f, row_group_metadata)))
  }
}

/// A serialized impl for row group reader
/// Here 'a is the lifetime for the row group metadata, which is owned
/// by the parent Parquet file reader
pub struct SerializedRowGroupReader<'a> {
  buf: BufReader<File>,
  metadata: &'a RowGroupMetaData
}

impl<'a> SerializedRowGroupReader<'a> {
  pub fn new(file: File, metadata: &'a RowGroupMetaData) -> Self {
    let buf = BufReader::new(file);
    Self { buf: buf, metadata: metadata }
  }
}

impl<'a> RowGroupReader<'a> for SerializedRowGroupReader<'a> {
  fn metadata(&self) -> &'a RowGroupMetaData {
    self.metadata
  }

  fn num_columns(&self) -> usize {
    self.metadata.num_columns()
  }

  // TODO: fix PARQUET-816
  fn get_page_reader<'b>(&'b self, i: usize) -> Result<Box<PageReader + 'b>> {
    let col = self.metadata.column(i);
    let mut col_start = col.data_page_offset();
    if col.has_dictionary_page() {
      col_start = col.dictionary_page_offset().unwrap();
    }
    let col_length = col.compressed_size() as u64;
    let f = self.buf.get_ref().try_clone()?;
    let mut buf = BufReader::new(f);
    let _ = buf.seek(SeekFrom::Start(col_start as u64));
    let page_reader = SerializedPageReader::new(
      buf.take(col_length).into_inner(), col.num_values(), col.compression())?;
    Ok(Box::new(page_reader))
  }
}


/// A serialized impl for Parquet page reader
pub struct SerializedPageReader {
  /// The buffer which contains exactly the bytes for the column trunk
  /// to be read by this page reader
  buf: BufReader<File>,

  /// The compression codec for this column chunk. Only set for
  /// non-PLAIN codec.
  decompressor: Option<Box<Codec>>,

  /// The number of values we have seen so far
  seen_num_values: i64,

  /// The number of total values in this column chunk
  total_num_values: i64,
}

impl SerializedPageReader {
  pub fn new(buf: BufReader<File>, total_num_values: i64,
             compression: Compression) -> Result<Self> {
    let decompressor = create_codec(compression)?;
    let result =
      Self { buf: buf, total_num_values: total_num_values, seen_num_values: 0,
             decompressor: decompressor };
    Ok(result)
  }

  fn read_page_header(&mut self) -> Result<PageHeader> {
    let transport = Rc::new(RefCell::new(
      Box::new(TMemoryBuffer::new(&mut self.buf)) as Box<TTransport>));
    let mut prot = TCompactInputProtocol::new(transport);
    let page_header = PageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
  }
}

impl PageReader for SerializedPageReader {
  fn get_next_page(&mut self) -> Result<Option<Page>> {
    while self.seen_num_values < self.total_num_values {
      let page_header = self.read_page_header()?;
      let compressed_len = page_header.compressed_page_size as usize;
      let uncompressed_len = page_header.uncompressed_page_size as usize;
      let mut buffer = ByteBuffer::new(compressed_len);
      self.buf.read_exact(buffer.mut_data())?;

      // TODO: page header could be huge because of statistics. We should
      // set a maximum page header size and abort if that is exceeded.
      if let Some(decompressor) = self.decompressor.as_mut() {
        let mut decompressed_buffer = vec!();
        let decompressed_size = decompressor.decompress(buffer.data(), &mut decompressed_buffer)?;
        if decompressed_size != uncompressed_len {
          return Err(decode_err!("Actual decompressed size doesn't \
            match the expected one ({} vs {})", decompressed_size, uncompressed_len));
        }
        buffer.set_data(decompressed_buffer);
      }

      // TODO: process statistics
      let result = match page_header.type_ {
        PageType::DICTIONARY_PAGE => {
          assert!(page_header.dictionary_page_header.is_some());
          let dict_header = page_header.dictionary_page_header.as_ref().unwrap();
          let is_sorted = match dict_header.is_sorted {
            None => false,
            Some(b) => b
          };
          self.seen_num_values += dict_header.num_values as i64;
          Page::DictionaryPage {
            buf: Box::new(buffer), num_values: dict_header.num_values as u32,
            encoding: Encoding::from(dict_header.encoding), is_sorted: is_sorted
          }
        },
        PageType::DATA_PAGE => {
          assert!(page_header.data_page_header.is_some());
          let header = page_header.data_page_header.as_ref().unwrap();
          self.seen_num_values += header.num_values as i64;
          Page::DataPage {
            buf: Box::new(buffer), num_values: header.num_values as u32,
            encoding: Encoding::from(header.encoding),
            def_level_encoding: Encoding::from(header.definition_level_encoding),
            rep_level_encoding: Encoding::from(header.repetition_level_encoding)
          }
        },
        PageType::DATA_PAGE_V2 => {
          assert!(page_header.data_page_header_v2.is_some());
          let header = page_header.data_page_header_v2.as_ref().unwrap();
          let is_compressed = match header.is_compressed {
            Some(b) => b,
            None => true
          };
          self.seen_num_values += header.num_values as i64;
          Page::DataPageV2 {
            buf: Box::new(buffer), num_values: header.num_values as u32,
            encoding: Encoding::from(header.encoding),
            num_nulls: header.num_nulls as u32, num_rows: header.num_rows as u32,
            def_levels_byte_len: header.definition_levels_byte_length as u32,
            rep_levels_byte_len: header.repetition_levels_byte_length as u32,
            is_compressed: is_compressed
          }
        },
        _ => {
          // For unknown page type (e.g., INDEX_PAGE), skip and read next.
          continue;
        }
      };
      return Ok(Some(result))
    }

    // We are at the end of this column chunk and no more page left. Return None.
    Ok(None)
  }
}

