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

use std::io::Write;

use basic::PageType;
use column::page::{CompressedPage, Page, PageWriteSpec, PageWriter};
use errors::Result;
use file::metadata::ColumnChunkMetaData;
use file::statistics::{to_thrift as statistics_to_thrift};
use parquet_format as parquet;
use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
use util::io::Position;

/// Serialized page writer.
///
/// Writes and serializes pages and metadata into output stream.
/// `SerializedPageWriter` should not be used after calling `close()`.
pub struct SerializedPageWriter<T: Write + Position> {
  sink: T
}

impl<T: Write + Position> SerializedPageWriter<T> {
  /// Creates new page writer.
  pub fn new(sink: T) -> Self {
    Self { sink: sink }
  }

  /// Serializes page header into Thrift.
  /// Returns number of bytes that have been written into the sink.
  #[inline]
  fn serialize_page_header(&mut self, header: parquet::PageHeader) -> Result<usize> {
    let start_pos = self.sink.pos();
    {
      let mut protocol = TCompactOutputProtocol::new(&mut self.sink);
      header.write_to_out_protocol(&mut protocol)?;
      protocol.flush()?;
    }
    Ok((self.sink.pos() - start_pos) as usize)
  }

  /// Serializes column chunk into Thrift.
  /// Returns Ok() if there are not errors serializing and writing data into the sink.
  #[inline]
  fn serialize_column_chunk(&mut self, chunk: parquet::ColumnChunk) -> Result<()> {
    let mut protocol = TCompactOutputProtocol::new(&mut self.sink);
    chunk.write_to_out_protocol(&mut protocol)?;
    protocol.flush()?;
    Ok(())
  }
}

impl<T: Write + Position> PageWriter for SerializedPageWriter<T> {
  fn write_page(&mut self, page: CompressedPage) -> Result<PageWriteSpec> {
    let uncompressed_size = page.uncompressed_size();
    let compressed_size = page.compressed_size();
    let num_values = page.num_values();
    let encoding = page.encoding();
    let page_type = page.page_type();

    let mut page_header = parquet::PageHeader {
      type_: page_type.into(),
      uncompressed_page_size: uncompressed_size as i32,
      compressed_page_size: compressed_size as i32,
      // TODO: Add support for crc checksum
      crc: None,
      data_page_header: None,
      index_page_header: None,
      dictionary_page_header: None,
      data_page_header_v2: None
    };

    match page.compressed_page() {
      &Page::DataPage {
        def_level_encoding,
        rep_level_encoding,
        ref statistics,
        ..
      } => {
        let data_page_header = parquet::DataPageHeader {
          num_values: num_values as i32,
          encoding: encoding.into(),
          definition_level_encoding: def_level_encoding.into(),
          repetition_level_encoding: rep_level_encoding.into(),
          statistics: statistics_to_thrift(statistics.as_ref())
        };
        page_header.data_page_header = Some(data_page_header);
      },
      &Page::DataPageV2 {
        num_nulls,
        num_rows,
        def_levels_byte_len,
        rep_levels_byte_len,
        is_compressed,
        ref statistics,
        ..
      } => {
        let data_page_header_v2 = parquet::DataPageHeaderV2 {
          num_values: num_values as i32,
          num_nulls: num_nulls as i32,
          num_rows: num_rows as i32,
          encoding: encoding.into(),
          definition_levels_byte_length: def_levels_byte_len as i32,
          repetition_levels_byte_length: rep_levels_byte_len as i32,
          is_compressed: Some(is_compressed),
          statistics: statistics_to_thrift(statistics.as_ref())
        };
        page_header.data_page_header_v2 = Some(data_page_header_v2);
      },
      &Page::DictionaryPage { is_sorted, .. } => {
        let dictionary_page_header = parquet::DictionaryPageHeader {
          num_values: num_values as i32,
          encoding: encoding.into(),
          is_sorted: Some(is_sorted)
        };
        page_header.dictionary_page_header = Some(dictionary_page_header);
      }
    }

    let start_pos = self.sink.pos();

    let header_size = self.serialize_page_header(page_header)?;
    self.sink.write_all(page.data())?;

    let mut spec = PageWriteSpec::new();
    spec.page_type = page_type;
    spec.uncompressed_size = uncompressed_size + header_size;
    spec.compressed_size = compressed_size + header_size;
    spec.offset = start_pos;
    spec.bytes_written = self.sink.pos() - start_pos;
    // Number of values is incremented for data pages only
    if page_type == PageType::DATA_PAGE || page_type == PageType::DATA_PAGE_V2 {
      spec.num_values = num_values;
    }

    Ok(spec)
  }

  fn write_metadata(&mut self, metadata: &ColumnChunkMetaData) -> Result<()> {
    self.serialize_column_chunk(metadata.to_thrift())
  }

  fn close(&mut self) -> Result<()> {
    self.sink.flush()?;
    Ok(())
  }
}


#[cfg(test)]
mod tests {
  use std::io::Cursor;

  use super::*;
  use basic::{Compression, Encoding, Type};
  use column::page::PageReader;
  use compression::{Codec, create_codec};
  use file::reader::SerializedPageReader;
  use file::statistics::{Statistics, from_thrift, to_thrift};
  use util::memory::ByteBufferPtr;

  #[test]
  fn test_page_writer_data_pages() {
    let pages = vec![
      Page::DataPage {
        buf: ByteBufferPtr::new(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        num_values: 10,
        encoding: Encoding::DELTA_BINARY_PACKED,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE,
        statistics: Some(Statistics::int32(Some(1), Some(3), None, 7, true))
      },
      Page::DataPageV2 {
        buf: ByteBufferPtr::new(vec![4; 128]),
        num_values: 10,
        encoding: Encoding::DELTA_BINARY_PACKED,
        num_nulls: 2,
        num_rows: 12,
        def_levels_byte_len: 24,
        rep_levels_byte_len: 32,
        is_compressed: false,
        statistics: Some(Statistics::int32(Some(1), Some(3), None, 7, true))
      }
    ];

    test_page_roundtrip(&pages[..], Compression::SNAPPY, Type::INT32);
    test_page_roundtrip(&pages[..], Compression::UNCOMPRESSED, Type::INT32);
  }

  #[test]
  fn test_page_writer_dict_pages() {
    let pages = vec![
      Page::DictionaryPage {
        buf: ByteBufferPtr::new(vec![1, 2, 3, 4, 5]),
        num_values: 5,
        encoding: Encoding::RLE_DICTIONARY,
        is_sorted: false
      },
      Page::DataPage {
        buf: ByteBufferPtr::new(vec![1, 2, 3, 4, 5, 6, 7, 8]),
        num_values: 10,
        encoding: Encoding::DELTA_BINARY_PACKED,
        def_level_encoding: Encoding::RLE,
        rep_level_encoding: Encoding::RLE,
        statistics: Some(Statistics::int32(Some(1), Some(3), None, 7, true))
      },
      Page::DataPageV2 {
        buf: ByteBufferPtr::new(vec![4; 128]),
        num_values: 10,
        encoding: Encoding::DELTA_BINARY_PACKED,
        num_nulls: 2,
        num_rows: 12,
        def_levels_byte_len: 24,
        rep_levels_byte_len: 32,
        is_compressed: false,
        statistics: None
      }
    ];

    test_page_roundtrip(&pages[..], Compression::SNAPPY, Type::INT32);
    test_page_roundtrip(&pages[..], Compression::UNCOMPRESSED, Type::INT32);
  }

  /// Tests writing and reading pages.
  /// Physical type is for statistics only, should match any defined statistics type in
  /// pages.
  fn test_page_roundtrip(
    pages: &[Page],
    codec: Compression,
    physical_type: Type
  ) {
    let mut compressed_pages = vec![];
    let mut total_num_values = 0i64;
    let mut compressor = create_codec(codec).unwrap();

    for page in pages {
      let uncompressed_len = page.buffer().len();

      let compressed_page = match page {
        &Page::DataPage {
          ref buf,
          num_values,
          encoding,
          def_level_encoding,
          rep_level_encoding,
          ref statistics
        } => {
          total_num_values += num_values as i64;
          let output_buf = compress_helper(compressor.as_mut(), buf.data());

          Page::DataPage {
            buf: ByteBufferPtr::new(output_buf),
            num_values: num_values,
            encoding: encoding,
            def_level_encoding: def_level_encoding,
            rep_level_encoding: rep_level_encoding,
            statistics: from_thrift(physical_type, to_thrift(statistics.as_ref()))
          }
        },
        &Page::DataPageV2 {
          ref buf,
          num_values,
          encoding,
          num_nulls,
          num_rows,
          def_levels_byte_len,
          rep_levels_byte_len,
          ref statistics,
          ..
        } => {
          total_num_values += num_values as i64;
          let offset = (def_levels_byte_len + rep_levels_byte_len) as usize;
          let cmp_buf = compress_helper(compressor.as_mut(), &buf.data()[offset..]);
          let mut output_buf = Vec::from(&buf.data()[..offset]);
          output_buf.extend_from_slice(&cmp_buf[..]);

          Page::DataPageV2 {
            buf: ByteBufferPtr::new(output_buf),
            num_values: num_values,
            encoding: encoding,
            num_nulls: num_nulls,
            num_rows: num_rows,
            def_levels_byte_len: def_levels_byte_len,
            rep_levels_byte_len: rep_levels_byte_len,
            is_compressed: compressor.is_some(),
            statistics: from_thrift(physical_type, to_thrift(statistics.as_ref()))
          }
        },
        &Page::DictionaryPage {
          ref buf,
          num_values,
          encoding,
          is_sorted
        } => {
          let output_buf = compress_helper(compressor.as_mut(), buf.data());

          Page::DictionaryPage {
            buf: ByteBufferPtr::new(output_buf),
            num_values: num_values,
            encoding: encoding,
            is_sorted
          }
        }
      };

      let compressed_page = CompressedPage::new(compressed_page, uncompressed_len);
      compressed_pages.push(compressed_page);
    }

    let mut buffer: Vec<u8> = vec![];
    let mut result_pages: Vec<Page> = vec![];
    {
      let cursor = Cursor::new(&mut buffer);
      let mut page_writer = SerializedPageWriter::new(cursor);

      for page in compressed_pages {
        page_writer.write_page(page).unwrap();
      }
      page_writer.close().unwrap();
    }
    {
      let mut page_reader = SerializedPageReader::new(
        Cursor::new(&buffer),
        total_num_values,
        codec,
        physical_type
      ).unwrap();

      while let Some(page) = page_reader.get_next_page().unwrap() {
        result_pages.push(page);
      }
    }

    assert_eq!(result_pages.len(), pages.len());
    for i in 0..result_pages.len() {
      assert_page(&result_pages[i], &pages[i]);
    }
  }

  /// Helper function to compress a slice
  fn compress_helper(compressor: Option<&mut Box<Codec>>, data: &[u8]) -> Vec<u8> {
    let mut output_buf = vec![];
    if let Some(cmpr) = compressor {
      cmpr.compress(data, &mut output_buf).unwrap();
    } else {
      output_buf.extend_from_slice(data);
    }
    output_buf
  }

  /// Check if pages match.
  fn assert_page(left: &Page, right: &Page) {
    assert_eq!(left.page_type(), right.page_type());
    assert_eq!(left.buffer().data(), right.buffer().data());
    assert_eq!(left.num_values(), right.num_values());
    assert_eq!(left.encoding(), right.encoding());
    assert_eq!(to_thrift(left.statistics()), to_thrift(right.statistics()));
  }
}
