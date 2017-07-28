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

use basic::*;
use data_type::*;
use schema::types::ColumnDescPtr;
use encodings::decoding::{get_decoder, Decoder};
use encodings::levels::LevelDecoder;
use errors::{Result, ParquetError};
use super::page::{Page, PageReader};

pub enum ColumnReader<'a> {
  BoolColumnReader(ColumnReaderImpl<'a, BoolType>),
  Int32ColumnReader(ColumnReaderImpl<'a, Int32Type>),
  Int64ColumnReader(ColumnReaderImpl<'a, Int64Type>),
  Int96ColumnReader(ColumnReaderImpl<'a, Int96Type>),
  FloatColumnReader(ColumnReaderImpl<'a, FloatType>),
  DoubleColumnReader(ColumnReaderImpl<'a, DoubleType>),
  ByteArrayColumnReader(ColumnReaderImpl<'a, ByteArrayType>),
  FixedLenByteArrayColumnReader(ColumnReaderImpl<'a, FixedLenByteArrayType>),
}

/// A value reader for a particular primitive column.
/// The lifetime parameter 'a denotes the lifetime of the page reader
pub struct ColumnReaderImpl<'a, T: DataType> {
  descr: ColumnDescPtr,
  def_level_decoder: Option<LevelDecoder>,
  rep_level_decoder: Option<LevelDecoder>,
  page_reader: Box<PageReader + 'a>,
  current_decoder: Option<Box<Decoder<T>>>,

  // The total number of values stored in the data page.
  num_buffered_values: u32,

  // The number of values from the current data page that has been
  // decoded into memory so far.
  num_decoded_values: u32,
}

impl<'a, T: DataType> ColumnReaderImpl<'a, T> where T: 'static {
  pub fn new(descr: ColumnDescPtr, page_reader: Box<PageReader + 'a>) -> Self {
    Self { descr: descr, def_level_decoder: None, rep_level_decoder: None,
           page_reader: page_reader, current_decoder: None,
           num_buffered_values: 0, num_decoded_values: 0 }
  }

  /// Read a new page. Returns false if there's no page left.
  pub fn read_new_page(&mut self) -> Result<bool> {
    let mut result = false;
    #[allow(while_true)]
    while true {
      let page_result: Option<Page> = self.page_reader.get_next_page()?;
      match page_result {
        // No more page to read
        None => {
          return Ok(false)
        },
        Some(current_page) => {
          match current_page {
            // 1. dictionary page: configure dictionary for this page.
            p @ Page::DictionaryPage{ .. } => {
              self.configure_dictionary(p)?;
            },
            // 2. data page v1
            Page::DataPage{ buf, num_values, encoding, def_level_encoding, rep_level_encoding } => {
              self.num_buffered_values = num_values;
              self.num_decoded_values = 0;

              let mut buffer_ptr = buf;

              if self.descr.max_rep_level() > 0 {
                let mut rep_decoder = LevelDecoder::new(rep_level_encoding, self.descr.max_rep_level());
                let total_bytes = rep_decoder.set_data(buffer_ptr.all());
                buffer_ptr = buffer_ptr.start_from(total_bytes);
                self.rep_level_decoder = Some(rep_decoder);
              }

              if self.descr.max_def_level() > 0 {
                let mut def_decoder = LevelDecoder::new(def_level_encoding, self.descr.max_def_level());
                let total_bytes = def_decoder.set_data(buffer_ptr.all());
                buffer_ptr = buffer_ptr.start_from(total_bytes);
                self.def_level_decoder = Some(def_decoder);
              }

              // Initialize decoder for this page
              let mut data_decoder = match encoding {
                Encoding::PLAIN => {
                  get_decoder::<T>(self.descr.clone(), encoding)?
                },
                en => {
                  return Err(nyi_err!("Unsupported encoding {}", en))
                }
              };
              data_decoder.set_data(buffer_ptr, num_values as usize)?;
              self.current_decoder = Some(data_decoder);
            },
            _ => {
              result = true;
            }
          };
          break;
        }
      }
    }
    Ok(result)
  }

  fn configure_dictionary(&mut self, _: Page) -> Result<bool> {
    Ok(false)
  }
}
