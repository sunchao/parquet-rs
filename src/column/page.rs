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

//! Contains Parquet Page definitions and page reader interface.

use basic::{PageType, Encoding};
use errors::Result;
use util::memory::ByteBufferPtr;

/// Parquet Page definition.
///
/// List of supported pages.
/// These are 1-to-1 mapped from the equivalent Thrift definitions, except `buf` which
/// used to store uncompressed bytes of the page.
pub enum Page {
  DataPage {
    buf: ByteBufferPtr,
    num_values: u32,
    encoding: Encoding,
    def_level_encoding: Encoding,
    rep_level_encoding: Encoding
  },
  DataPageV2 {
    buf: ByteBufferPtr,
    num_values: u32,
    encoding: Encoding,
    num_nulls: u32,
    num_rows: u32,
    def_levels_byte_len: u32,
    rep_levels_byte_len: u32,
    is_compressed: bool
  },
  DictionaryPage {
    buf: ByteBufferPtr,
    num_values: u32,
    encoding: Encoding,
    is_sorted: bool
  }
}

impl Page {
  /// Returns [`PageType`](`::basic::PageType`) for this page.
  pub fn page_type(&self) -> PageType {
    match self {
      &Page::DataPage { .. } => PageType::DATA_PAGE,
      &Page::DataPageV2 { .. } => PageType::DATA_PAGE_V2,
      &Page::DictionaryPage { .. } => PageType::DICTIONARY_PAGE
    }
  }

  /// Returns internal byte buffer reference for this page.
  pub fn buffer(&self) -> &ByteBufferPtr {
    match self {
      &Page::DataPage { ref buf, .. } => &buf,
      &Page::DataPageV2 { ref buf, .. } => &buf,
      &Page::DictionaryPage { ref buf, .. } => &buf
    }
  }

  /// Returns number of values in this page.
  pub fn num_values(&self) -> u32 {
    match self {
      &Page::DataPage { num_values, .. } => num_values,
      &Page::DataPageV2 { num_values, .. } => num_values,
      &Page::DictionaryPage { num_values, .. } => num_values
    }
  }

  /// Returns this page [`Encoding`](`::basic::Encoding`).
  pub fn encoding(&self) -> Encoding {
    match self {
      &Page::DataPage { encoding, .. } => encoding,
      &Page::DataPageV2 { encoding, .. } => encoding,
      &Page::DictionaryPage { encoding, .. } => encoding
    }
  }
}

/// API for reading pages from a column chunk.
/// This offers a iterator like API to get the next page.
pub trait PageReader {
  /// Gets the next page in the column chunk associated with this reader.
  /// Returns `None` if there are no pages left.
  fn get_next_page(&mut self) -> Result<Option<Page>>;
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_page() {
    let data_page = Page::DataPage {
      buf: ByteBufferPtr::new(vec![0, 1, 2]),
      num_values: 10,
      encoding: Encoding::PLAIN,
      def_level_encoding: Encoding::RLE,
      rep_level_encoding: Encoding::RLE
    };
    assert_eq!(data_page.page_type(), PageType::DATA_PAGE);
    assert_eq!(data_page.buffer().data(), vec![0, 1, 2].as_slice());
    assert_eq!(data_page.num_values(), 10);
    assert_eq!(data_page.encoding(), Encoding::PLAIN);

    let data_page_v2 = Page::DataPageV2 {
      buf: ByteBufferPtr::new(vec![0, 1, 2]),
      num_values: 10,
      encoding: Encoding::PLAIN,
      num_nulls: 5,
      num_rows: 20,
      def_levels_byte_len: 30,
      rep_levels_byte_len: 40,
      is_compressed: false
    };
    assert_eq!(data_page_v2.page_type(), PageType::DATA_PAGE_V2);
    assert_eq!(data_page_v2.buffer().data(), vec![0, 1, 2].as_slice());
    assert_eq!(data_page_v2.num_values(), 10);
    assert_eq!(data_page_v2.encoding(), Encoding::PLAIN);

    let dict_page = Page::DictionaryPage {
      buf: ByteBufferPtr::new(vec![0, 1, 2]),
      num_values: 10,
      encoding: Encoding::PLAIN,
      is_sorted: false
    };
    assert_eq!(dict_page.page_type(), PageType::DICTIONARY_PAGE);
    assert_eq!(dict_page.buffer().data(), vec![0, 1, 2].as_slice());
    assert_eq!(dict_page.num_values(), 10);
    assert_eq!(dict_page.encoding(), Encoding::PLAIN);
  }
}
