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

use basic::{PageType, Encoding};
use errors::Result;
use util::memory::Buffer;

pub enum Page {
  DataPage {
    buf: Box<Buffer>, num_values: u32, encoding: Encoding,
    def_level_encoding: Encoding, rep_level_encoding: Encoding
  },
  DataPageV2 {
    buf: Box<Buffer>, num_values: u32,  encoding: Encoding,
    num_nulls: u32, num_rows: u32,
    def_levels_byte_len: u32, rep_levels_byte_len: u32,
    is_compressed: bool
  },
  DictionaryPage {
    buf: Box<Buffer>, num_values: u32, encoding: Encoding, is_sorted: bool
  }
}

impl Page {
  pub fn page_type(&self) -> PageType {
    match self {
      &Page::DataPage{ .. } => PageType::DATA_PAGE,
      &Page::DataPageV2{ .. } => PageType::DATA_PAGE_V2,
      &Page::DictionaryPage{ .. } => PageType::DICTIONARY_PAGE
    }
  }

  pub fn buffer(&self) -> &Box<Buffer> {
    match self {
      &Page::DataPage{ ref buf, .. } => buf,
      &Page::DataPageV2{ ref buf, .. } => buf,
      &Page::DictionaryPage{ ref buf, .. } => buf
    }
  }

  pub fn num_values(&self) -> u32 {
    match self {
      &Page::DataPage{ num_values, .. } => num_values,
      &Page::DataPageV2{ num_values, .. } => num_values,
      &Page::DictionaryPage{ num_values, .. } => num_values
    }
  }

  pub fn encoding(&self) -> Encoding {
    match self {
      &Page::DataPage{ encoding, .. } => encoding,
      &Page::DataPageV2{ encoding, .. } => encoding,
      &Page::DictionaryPage{ encoding, .. } => encoding
    }
  }
}


/// API for reading pages from a column chunk. This offers a iterator
/// like API to get the next page.
pub trait PageReader {
  /// Get the next page in the column chunk associated with this reader.
  /// Return `None` if there's no page left.
  fn get_next_page(&mut self) -> Result<Option<Page>>;
}
