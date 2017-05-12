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
use encodings::decoding::{get_decoder, Decoder, ValueType};
use util::memory::{BytePtr, MemoryPool};
use errors::{Result, ParquetError};
use super::page::{Page, PageReader};

pub enum ColumnReader<'a, 'm> {
  BoolColumnReader(ColumnReaderImpl<'a, 'm, BoolType>),
  Int32ColumnReader(ColumnReaderImpl<'a, 'm, Int32Type>),
  Int64ColumnReader(ColumnReaderImpl<'a, 'm, Int64Type>),
  Int96ColumnReader(ColumnReaderImpl<'a, 'm, Int96Type>),
  FloatColumnReader(ColumnReaderImpl<'a, 'm, FloatType>),
  DoubleColumnReader(ColumnReaderImpl<'a, 'm, DoubleType>),
  ByteArrayColumnReader(ColumnReaderImpl<'a, 'm, ByteArrayType>),
  FixedLenByteArrayColumnReader(ColumnReaderImpl<'a, 'm, FixedLenByteArrayType>),
}

/// A value reader for a particular primitive column.
/// Lifetime parameters:
///   1. 'a is the lifetime of the page reader
///   2. 'm is the lifetime of the memory pool
pub struct ColumnReaderImpl<'a, 'm, T: DataType> where T: 'm {
  descr: ColumnDescPtr,
  def_level_decoder: Option<Box<Decoder<Int32Type> + 'm>>,
  rep_level_decoder: Option<Box<Decoder<Int32Type> + 'm>>,
  page_reader: Box<PageReader + 'a>,
  current_decoder: Option<Box<Decoder<T> + 'm>>,

  // The total number of values stored in the data page.
  num_buffered_values: u32,

  // The number of values from the current data page that has been
  // decoded into memory so far.
  num_decoded_values: u32,

  memory_pool: &'m MemoryPool
}

impl<'a, 'm, T: DataType> ColumnReaderImpl<'a, 'm, T> where T: 'm {
  pub fn new(descr: ColumnDescPtr, page_reader: Box<PageReader + 'a>, mem_pool: &'m MemoryPool) -> Self {
    Self { descr: descr, def_level_decoder: None, rep_level_decoder: None,
           page_reader: page_reader, current_decoder: None,
           num_buffered_values: 0, num_decoded_values: 0, memory_pool: mem_pool }
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

              let mut buffer_ptr = BytePtr::new(buf);

              if self.descr.max_def_level() > 0 {
                let mut def_decoder = get_decoder::<'m, Int32Type>(
                  self.descr.clone(), def_level_encoding, ValueType::DEF_LEVEL, &self.memory_pool)?;
                def_decoder.set_data(buffer_ptr.all(), num_values as usize)?;
                buffer_ptr = buffer_ptr.start_from(def_decoder.total_bytes()?);
                self.def_level_decoder = Some(def_decoder);
              }

              if self.descr.max_rep_level() > 0 {
                let mut rep_decoder = get_decoder::<'m, Int32Type>(
                  self.descr.clone(), rep_level_encoding, ValueType::REP_LEVEL, &self.memory_pool)?;
                rep_decoder.set_data(buffer_ptr.all(), num_values as usize)?;
                buffer_ptr = buffer_ptr.start_from(rep_decoder.total_bytes()?);
                self.rep_level_decoder = Some(rep_decoder);
              }

              // Initialize decoder for this page
              let mut data_decoder = match encoding {
                Encoding::PLAIN => {
                  get_decoder::<'m, T>(self.descr.clone(), encoding, ValueType::VALUE, &self.memory_pool)?
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
