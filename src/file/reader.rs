
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

//! Contains file reader API, and provides methods to access file metadata, row group
//! readers to read individual column chunks, or access record iterator.

use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::rc::Rc;

use basic::{Type, Compression, Encoding};
use byteorder::{LittleEndian, ByteOrder};
use column::page::{Page, PageReader};
use column::reader::{ColumnReader, ColumnReaderImpl};
use compression::{create_codec, Codec};
use errors::{ParquetError, Result};
use file::metadata::*;
use parquet_format::FileMetaData as TFileMetaData;
use parquet_format::{PageType, PageHeader};
use record::reader::RowIter;
use schema::types::{self, SchemaDescriptor, Type as SchemaType};
use thrift::protocol::TCompactInputProtocol;
use util::io::FileChunk;
use util::memory::ByteBufferPtr;

// ----------------------------------------------------------------------
// APIs for file & row group readers

/// Parquet file reader API. With this, user can get metadata information about the
/// Parquet file, can get reader for each row group, and access record iterator.
pub trait FileReader {
  /// Get metadata information about this file.
  fn metadata(&self) -> ParquetMetaDataPtr;

  /// Get the total number of row groups for this file.
  fn num_row_groups(&self) -> usize;

  /// Get the `i`th row group reader. Note this doesn't do bound check.
  fn get_row_group(&self, i: usize) -> Result<Box<RowGroupReader>>;

  /// Get full iterator of `Row`s from a file (over all row groups).
  ///
  /// Iterator will automatically load the next row group to advance.
  ///
  /// Projected schema can be a subset of or equal to the file schema, when it is None,
  /// full file schema is assumed.
  fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter>;
}

/// Parquet row group reader API. With this, user can get metadata information about the
/// row group, as well as readers for each individual column chunk.
pub trait RowGroupReader {
  /// Get metadata information about this row group.
  fn metadata(&self) -> RowGroupMetaDataPtr;

  /// Get the total number of column chunks in this row group.
  fn num_columns(&self) -> usize;

  /// Get page reader for the `i`th column chunk.
  fn get_column_page_reader(&self, i: usize) -> Result<Box<PageReader>>;

  /// Get value reader for the `i`th column chunk.
  fn get_column_reader(&self, i: usize) -> Result<ColumnReader>;

  /// Get iterator of `Row`s from this row group.
  ///
  /// Projected schema can be a subset of or equal to the file schema, when it is None,
  /// full file schema is assumed.
  fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter>;
}

/// A thin wrapper on `T: Read` to be used by Thrift transport. Write is not supported.
struct TMemoryBuffer<'a, T> where T: 'a + Read {
  data: &'a mut T
}

impl<'a, T: 'a + Read> TMemoryBuffer<'a, T> {
  fn new(data: &'a mut T) -> Self {
    Self { data: data }
  }
}

impl<'a, T: 'a + Read> Read for TMemoryBuffer<'a, T> {
  fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
    let bytes_read = self.data.read(buf)?;
    Ok(bytes_read)
  }
}

// ----------------------------------------------------------------------
// Serialized impl for file & row group readers

const FOOTER_SIZE: usize = 8;
const PARQUET_MAGIC: [u8; 4] = [b'P', b'A', b'R', b'1'];

/// A serialized implementation for Parquet [`FileReader`].
pub struct SerializedFileReader {
  buf: BufReader<File>,
  metadata: ParquetMetaDataPtr
}

impl SerializedFileReader {
  /// Creates file reader from a Parquet file.
  /// Returns error if Parquet file does not exist or is corrupt.
  pub fn new(file: File) -> Result<Self> {
    let mut buf = BufReader::new(file);
    let metadata = Self::parse_metadata(&mut buf)?;
    Ok(Self { buf: buf, metadata: Rc::new(metadata) })
  }

  // Layout of Parquet file
  // +---------------------------+---+-----+
  // |      Rest of file         | B |  A  |
  // +---------------------------+---+-----+
  // where A: parquet footer, B: parquet metadata.
  //
  fn parse_metadata(buf: &mut BufReader<File>) -> Result<ParquetMetaData> {
    let file_metadata = buf.get_ref().metadata()?;
    let file_size = file_metadata.len();
    if file_size < (FOOTER_SIZE as u64) {
      return Err(general_err!("Invalid Parquet file. Size is smaller than footer"));
    }
    let mut footer_buffer: [u8; FOOTER_SIZE] = [0; FOOTER_SIZE];
    buf.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
    buf.read_exact(&mut footer_buffer)?;
    if footer_buffer[4..] != PARQUET_MAGIC {
      return Err(general_err!("Invalid Parquet file. Corrupt footer"));
    }
    let metadata_len = LittleEndian::read_i32(&footer_buffer[0..4]) as i64;
    if metadata_len < 0 {
      return Err(general_err!(
        "Invalid Parquet file. Metadata length is less than zero ({})",
        metadata_len
      ));
    }
    let metadata_start: i64 = file_size as i64 - FOOTER_SIZE as i64 - metadata_len;
    if metadata_start < 0 {
      return Err(general_err!(
        "Invalid Parquet file. Metadata start is less than zero ({})",
        metadata_start
      ));
    }
    buf.seek(SeekFrom::Start(metadata_start as u64))?;
    let metadata_buf = buf.take(metadata_len as u64).into_inner();
    let transport = TMemoryBuffer::new(metadata_buf);

    // TODO: row group filtering
    let mut prot = TCompactInputProtocol::new(transport);
    let mut t_file_metadata: TFileMetaData =
      TFileMetaData::read_from_in_protocol(&mut prot)
        .map_err(|e| ParquetError::General(format!("Could not parse metadata: {}", e)))?;
    let schema = types::from_thrift(&mut t_file_metadata.schema)?;
    let schema_descr = Rc::new(SchemaDescriptor::new(schema.clone()));
    let mut row_groups = Vec::new();
    for rg in t_file_metadata.row_groups {
      row_groups.push(RowGroupMetaData::from_thrift(schema_descr.clone(), rg)?);
    }

    let file_metadata = FileMetaData::new(
      t_file_metadata.version,
      t_file_metadata.num_rows,
      t_file_metadata.created_by,
      schema,
      schema_descr
    );
    Ok(ParquetMetaData::new(file_metadata, row_groups))
  }
}

impl FileReader for SerializedFileReader {
  fn metadata(&self) -> ParquetMetaDataPtr {
    self.metadata.clone()
  }

  fn num_row_groups(&self) -> usize {
    self.metadata.num_row_groups()
  }

  fn get_row_group(&self, i: usize) -> Result<Box<RowGroupReader>> {
    let row_group_metadata = self.metadata.row_group(i);
    // Row groups should be processed sequentially.
    let f = self.buf.get_ref().try_clone()?;
    Ok(Box::new(SerializedRowGroupReader::new(f, row_group_metadata)))
  }

  fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
    RowIter::from_file(projection, self)
  }
}

/// A serialized implementation for Parquet [`RowGroupReader`].
pub struct SerializedRowGroupReader {
  buf: BufReader<File>,
  metadata: RowGroupMetaDataPtr
}

impl SerializedRowGroupReader {
  /// Creates new row group reader from a file and row group metadata.
  fn new(file: File, metadata: RowGroupMetaDataPtr) -> Self {
    let buf = BufReader::new(file);
    Self { buf, metadata }
  }
}

impl RowGroupReader for SerializedRowGroupReader {
  fn metadata(&self) -> RowGroupMetaDataPtr {
    self.metadata.clone()
  }

  fn num_columns(&self) -> usize {
    self.metadata.num_columns()
  }

  // TODO: fix PARQUET-816
  fn get_column_page_reader(&self, i: usize) -> Result<Box<PageReader>> {
    let col = self.metadata.column(i);
    let mut col_start = col.data_page_offset();
    if col.has_dictionary_page() {
      col_start = col.dictionary_page_offset().unwrap();
    }
    let col_length = col.compressed_size();
    let file_chunk = FileChunk::new(
      self.buf.get_ref(), col_start as usize, col_length as usize);
    let page_reader = SerializedPageReader::new(
      file_chunk, col.num_values(), col.compression())?;
    Ok(Box::new(page_reader))
  }

  fn get_column_reader(&self, i: usize) -> Result<ColumnReader> {
    let schema_descr = self.metadata.schema_descr();
    let col_descr = schema_descr.column(i);
    let col_page_reader = self.get_column_page_reader(i)?;
    let col_reader = match col_descr.physical_type() {
      Type::BOOLEAN => ColumnReader::BoolColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
      Type::INT32 => ColumnReader::Int32ColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
      Type::INT64 => ColumnReader::Int64ColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
      Type::INT96 => ColumnReader::Int96ColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
      Type::FLOAT => ColumnReader::FloatColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
      Type::DOUBLE => ColumnReader::DoubleColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
      Type::BYTE_ARRAY => ColumnReader::ByteArrayColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
      Type::FIXED_LEN_BYTE_ARRAY => ColumnReader::FixedLenByteArrayColumnReader(
        ColumnReaderImpl::new(col_descr, col_page_reader)),
    };
    Ok(col_reader)
  }

  fn get_row_iter(&self, projection: Option<SchemaType>) -> Result<RowIter> {
    RowIter::from_row_group(projection, self)
  }
}

/// A serialized implementation for Parquet [`PageReader`].
pub struct SerializedPageReader {
  // The file chunk buffer which references exactly the bytes for the column trunk
  // to be read by this page reader.
  buf: FileChunk,

  // The compression codec for this column chunk. Only set for non-PLAIN codec.
  decompressor: Option<Box<Codec>>,

  // The number of values we have seen so far.
  seen_num_values: i64,

  // The number of total values in this column chunk.
  total_num_values: i64
}

impl SerializedPageReader {
  /// Creates a new serialized page reader from file chunk.
  fn new(
    buf: FileChunk,
    total_num_values: i64,
    compression: Compression
  ) -> Result<Self> {
    let decompressor = create_codec(compression)?;
    let result = Self {
      buf: buf,
      total_num_values: total_num_values,
      seen_num_values: 0,
      decompressor: decompressor
    };
    Ok(result)
  }

  /// Reads Page header from Thrift.
  fn read_page_header(&mut self) -> Result<PageHeader> {
    let transport = TMemoryBuffer::new(&mut self.buf);
    let mut prot = TCompactInputProtocol::new(transport);
    let page_header = PageHeader::read_from_in_protocol(&mut prot)?;
    Ok(page_header)
  }
}

impl PageReader for SerializedPageReader {
  fn get_next_page(&mut self) -> Result<Option<Page>> {
    while self.seen_num_values < self.total_num_values {
      let page_header = self.read_page_header()?;

      // When processing data page v2, depending on enabled compression for the page, we
      // should account for uncompressed data ('offset') of repetition and definition
      // levels.
      //
      // We always use 0 offset for other pages other than v2, `true` flag means that
      // compression will be applied if decompressor is defined
      let mut offset: usize = 0;
      let mut can_decompress = true;

      if let Some(ref header_v2) = page_header.data_page_header_v2 {
        offset = (header_v2.definition_levels_byte_length +
          header_v2.repetition_levels_byte_length) as usize;
        // When is_compressed flag is missing the page is considered compressed
        can_decompress = header_v2.is_compressed.unwrap_or(true);
      }

      let compressed_len = page_header.compressed_page_size as usize - offset;
      let uncompressed_len = page_header.uncompressed_page_size as usize - offset;
      // We still need to read all bytes from buffered stream
      let mut buffer = vec![0; offset + compressed_len];
      self.buf.read_exact(&mut buffer)?;

      // TODO: page header could be huge because of statistics. We should set a maximum
      // page header size and abort if that is exceeded.
      if let Some(decompressor) = self.decompressor.as_mut() {
        if can_decompress {
          let mut decompressed_buffer = vec![];
          let decompressed_size =
            decompressor.decompress(&buffer[offset..], &mut decompressed_buffer)?;
          if decompressed_size != uncompressed_len {
            return Err(general_err!(
              "Actual decompressed size doesn't \
               match the expected one ({} vs {})",
              decompressed_size,
              uncompressed_len
            ));
          }
          if offset == 0 {
            buffer = decompressed_buffer;
          } else {
            // Prepend saved offsets to the buffer
            buffer.truncate(offset);
            buffer.append(&mut decompressed_buffer);
          }
        }
      }

      // TODO: process statistics
      let result = match page_header.type_ {
        PageType::DICTIONARY_PAGE => {
          assert!(page_header.dictionary_page_header.is_some());
          let dict_header = page_header.dictionary_page_header.as_ref().unwrap();
          let is_sorted = dict_header.is_sorted.unwrap_or(false);
          Page::DictionaryPage {
            buf: ByteBufferPtr::new(buffer),
            num_values: dict_header.num_values as u32,
            encoding: Encoding::from(dict_header.encoding),
            is_sorted: is_sorted
          }
        },
        PageType::DATA_PAGE => {
          assert!(page_header.data_page_header.is_some());
          let header = page_header.data_page_header.as_ref().unwrap();
          self.seen_num_values += header.num_values as i64;
          Page::DataPage {
            buf: ByteBufferPtr::new(buffer),
            num_values: header.num_values as u32,
            encoding: Encoding::from(header.encoding),
            def_level_encoding: Encoding::from(header.definition_level_encoding),
            rep_level_encoding: Encoding::from(header.repetition_level_encoding)
          }
        },
        PageType::DATA_PAGE_V2 => {
          assert!(page_header.data_page_header_v2.is_some());
          let header = page_header.data_page_header_v2.as_ref().unwrap();
          let is_compressed = header.is_compressed.unwrap_or(true);
          self.seen_num_values += header.num_values as i64;
          Page::DataPageV2 {
            buf: ByteBufferPtr::new(buffer),
            num_values: header.num_values as u32,
            encoding: Encoding::from(header.encoding),
            num_nulls: header.num_nulls as u32,
            num_rows: header.num_rows as u32,
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
      return Ok(Some(result));
    }

    // We are at the end of this column chunk and no more page left. Return None.
    Ok(None)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use util::test_common::{get_temp_file, get_test_file};

  #[test]
  fn test_file_reader_metadata_size_smaller_than_footer() {
    let test_file = get_temp_file("corrupt-1.parquet", &[]);
    let reader_result = SerializedFileReader::new(test_file);
    assert!(reader_result.is_err());
    assert_eq!(
      reader_result.err().unwrap(),
      general_err!("Invalid Parquet file. Size is smaller than footer")
    );
  }

  #[test]
  fn test_file_reader_metadata_corrupt_footer() {
    let test_file = get_temp_file("corrupt-2.parquet", &[1, 2, 3, 4, 5, 6, 7, 8]);
    let reader_result = SerializedFileReader::new(test_file);
    assert!(reader_result.is_err());
    assert_eq!(
      reader_result.err().unwrap(),
      general_err!("Invalid Parquet file. Corrupt footer")
    );
  }

  #[test]
  fn test_file_reader_metadata_invalid_length() {
    let test_file =
      get_temp_file("corrupt-3.parquet", &[0, 0, 0, 255, b'P', b'A', b'R', b'1']);
    let reader_result = SerializedFileReader::new(test_file);
    assert!(reader_result.is_err());
    assert_eq!(
      reader_result.err().unwrap(),
      general_err!("Invalid Parquet file. Metadata length is less than zero (-16777216)")
    );
  }

  #[test]
  fn test_file_reader_metadata_invalid_start() {
    let test_file =
      get_temp_file("corrupt-4.parquet", &[255, 0, 0, 0, b'P', b'A', b'R', b'1']);
    let reader_result = SerializedFileReader::new(test_file);
    assert!(reader_result.is_err());
    assert_eq!(
      reader_result.err().unwrap(),
      general_err!("Invalid Parquet file. Metadata start is less than zero (-255)")
    );
  }

  #[test]
  fn test_reuse_file_chunk() {
    // This test covers the case of maintaining the correct start position in a file
    // stream for each column reader after initializing and moving to the next one
    // (without necessarily reading the entire column).
    let test_file = get_test_file("alltypes_plain.parquet");
    let reader = SerializedFileReader::new(test_file).unwrap();
    let row_group = reader.get_row_group(0).unwrap();

    let mut page_readers = Vec::new();
    for i in 0..row_group.num_columns() {
      page_readers.push(row_group.get_column_page_reader(i).unwrap());
    }

    // Now buffer each col reader, we do not expect any failures like:
    // General("underlying Thrift error: end of file")
    for mut page_reader in page_readers {
      assert!(page_reader.get_next_page().is_ok());
    }
  }

  #[test]
  fn test_file_reader() {
    let test_file = get_test_file("alltypes_plain.parquet");
    let reader_result = SerializedFileReader::new(test_file);
    assert!(reader_result.is_ok());
    let reader = reader_result.unwrap();

    // Test contents in Parquet metadata
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), 1);

    // Test contents in file metadata
    let file_metadata = metadata.file_metadata();
    assert!(file_metadata.created_by().is_some());
    assert_eq!(
      file_metadata.created_by().as_ref().unwrap(),
      "impala version 1.3.0-INTERNAL (build 8a48ddb1eff84592b3fc06bc6f51ec120e1fffc9)"
    );
    assert_eq!(file_metadata.num_rows(), 8);
    assert_eq!(file_metadata.version(), 1);

    // Test contents in row group metadata
    let row_group_metadata = metadata.row_group(0);
    assert_eq!(row_group_metadata.num_columns(), 11);
    assert_eq!(row_group_metadata.num_rows(), 8);
    assert_eq!(row_group_metadata.total_byte_size(), 671);

    // Test row group reader
    let row_group_reader_result = reader.get_row_group(0);
    assert!(row_group_reader_result.is_ok());
    let row_group_reader: Box<RowGroupReader> = row_group_reader_result.unwrap();
    assert_eq!(row_group_reader.num_columns(), row_group_metadata.num_columns());
    assert_eq!(
      row_group_reader.metadata().total_byte_size(),
      row_group_metadata.total_byte_size()
    );

    // Test page readers
    // TODO: test for every column
    let page_reader_0_result = row_group_reader.get_column_page_reader(0);
    assert!(page_reader_0_result.is_ok());
    let mut page_reader_0: Box<PageReader> = page_reader_0_result.unwrap();
    let mut page_count = 0;
    while let Ok(Some(page)) = page_reader_0.get_next_page() {
      let is_expected_page = match page {
        Page::DictionaryPage {
          buf,
          num_values,
          encoding,
          is_sorted
        } => {
          assert_eq!(buf.len(), 32);
          assert_eq!(num_values, 8);
          assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
          assert_eq!(is_sorted, false);
          true
        },
        Page::DataPage {
          buf,
          num_values,
          encoding,
          def_level_encoding,
          rep_level_encoding
        } => {
          assert_eq!(buf.len(), 11);
          assert_eq!(num_values, 8);
          assert_eq!(encoding, Encoding::PLAIN_DICTIONARY);
          assert_eq!(def_level_encoding, Encoding::RLE);
          assert_eq!(rep_level_encoding, Encoding::BIT_PACKED);
          true
        },
        _ => false
      };
      assert!(is_expected_page);
      page_count += 1;
    }
    assert_eq!(page_count, 2);
  }

  #[test]
  fn test_file_reader_datapage_v2() {
    let test_file = get_test_file("test_datapage_v2.snappy.parquet");
    let reader_result = SerializedFileReader::new(test_file);
    assert!(reader_result.is_ok());
    let reader = reader_result.unwrap();

    // Test contents in Parquet metadata
    let metadata = reader.metadata();
    assert_eq!(metadata.num_row_groups(), 1);

    // Test contents in file metadata
    let file_metadata = metadata.file_metadata();
    assert!(file_metadata.created_by().is_some());
    assert_eq!(
      file_metadata.created_by().as_ref().unwrap(),
      "parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)"
    );
    assert_eq!(file_metadata.num_rows(), 5);
    assert_eq!(file_metadata.version(), 1);

    let row_group_metadata = metadata.row_group(0);

    // Test row group reader
    let row_group_reader_result = reader.get_row_group(0);
    assert!(row_group_reader_result.is_ok());
    let row_group_reader: Box<RowGroupReader> = row_group_reader_result.unwrap();
    assert_eq!(row_group_reader.num_columns(), row_group_metadata.num_columns());
    assert_eq!(
      row_group_reader.metadata().total_byte_size(),
      row_group_metadata.total_byte_size()
    );

    // Test page readers
    // TODO: test for every column
    let page_reader_0_result = row_group_reader.get_column_page_reader(0);
    assert!(page_reader_0_result.is_ok());
    let mut page_reader_0: Box<PageReader> = page_reader_0_result.unwrap();
    let mut page_count = 0;
    while let Ok(Some(page)) = page_reader_0.get_next_page() {
      let is_expected_page = match page {
        Page::DictionaryPage {
          buf,
          num_values,
          encoding,
          is_sorted
        } => {
          assert_eq!(buf.len(), 7);
          assert_eq!(num_values, 1);
          assert_eq!(encoding, Encoding::PLAIN);
          assert_eq!(is_sorted, false);
          true
        },
        Page::DataPageV2 {
          buf,
          num_values,
          encoding,
          num_nulls,
          num_rows,
          def_levels_byte_len,
          rep_levels_byte_len,
          is_compressed
        } => {
          assert_eq!(buf.len(), 4);
          assert_eq!(num_values, 5);
          assert_eq!(encoding, Encoding::RLE_DICTIONARY);
          assert_eq!(num_nulls, 1);
          assert_eq!(num_rows, 5);
          assert_eq!(def_levels_byte_len, 2);
          assert_eq!(rep_levels_byte_len, 0);
          assert_eq!(is_compressed, true);
          true
        },
        _ => false
      };
      assert!(is_expected_page);
      page_count += 1;
    }
    assert_eq!(page_count, 2);
  }
}
