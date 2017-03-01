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

use std::mem;
use std::cmp;
use std::slice::from_raw_parts_mut;
use basic::{Encoding, Type as PhysicalType, DataType, LogicalType, Repetition};
use errors::{Result, ParquetError};
use file::metadata::ColumnPath;
use schema::types::ColumnDescriptor;

// ----------------------------------------------------------------------
// Decoders

pub trait Decoder<'a> {
  /// Set the data to decode to be `data`, which should contain `num_values` of
  /// values to decode
  fn set_data(&mut self, num_values: usize, data: &'a mut [u8]);

  /// Try to consume at most `max_values` from this decoder and write
  /// the result to `buffer`. Return the actual number of values written.
  /// N.B., `buffer` should have at least `max_values` capacity.
  fn decode<E>(&mut self, buffer: &mut [E], max_values: usize) -> Result<usize>;

  /// Return the number of values left in the current buffer to decode
  fn values_left(&self) -> usize;

  /// Return the encoding for this decoder
  fn encoding(&self) -> Encoding;
}

pub struct PlainDecoder<'a> {
  /// A column descriptor about the primitive type this decoder is for
  descriptor: ColumnDescriptor,

  /// The encoding of the source data
  encoding: Encoding,

  /// The byte array to decode from
  data: &'a mut[u8],

  /// The remaining number of values in the byte array
  num_values: usize,

  /// The current starting index in the byte array.
  start: usize,

  /// the length of the type to be decoded
  type_length: usize
}

impl<'a> PlainDecoder<'a> {
  pub fn new(desc: ColumnDescriptor, encoding: Encoding, data: &'a mut[u8],
             num_values: usize, type_length: usize) -> Self {
    PlainDecoder{
      descriptor: desc, encoding: encoding, data: data,
      num_values: num_values, start: 0, type_length: type_length
    }
  }
}

impl<'a> Decoder<'a> for PlainDecoder<'a> {
  default fn set_data(&mut self, num_values: usize, data: &'a mut[u8]) {
    self.num_values = num_values;
    self.data = data;
  }

  #[inline]
  default fn values_left(&self) -> usize {
    self.num_values
  }

  #[inline]
  default fn encoding(&self) -> Encoding {
    self.encoding
  }

  #[inline]
  default fn decode<E>(&mut self, buffer: &mut [E], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    let num_values = cmp::min(max_values, self.num_values);
    let bytes_left = self.data.len() - self.start;
    let bytes_to_decode = mem::size_of::<E>() * num_values;
    if bytes_left < bytes_to_decode {
      return Err(schema_err!("Not enough bytes to decode (requested: {}, actual: {})",
                             bytes_to_decode, bytes_left));
    }
    let raw_buffer: &mut [u8] = unsafe {
      from_raw_parts_mut(buffer.as_ptr() as *mut u8, num_values * mem::size_of::<E>())
    };
    raw_buffer.copy_from_slice(&self.data[self.start..self.start + bytes_to_decode]);
    self.start += bytes_to_decode;
    Ok(num_values)
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::slice::from_raw_parts;
  use schema::types::PrimitiveType;

  #[test]
  fn test_decode() {
    let mut pty = PrimitiveType::new(
      "foo", Repetition::OPTIONAL, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, Some(0)).unwrap();
    let desc = ColumnDescriptor::new(ColumnPath::new(vec![String::from("foo")]), pty, 0, 0);
    let mut data = vec![42, 0, 0, 0, 18, 0, 0, 0, 52, 0, 0, 0];
    let mut decoder = PlainDecoder::new(desc, Encoding::PLAIN, data.as_mut_slice(), 3, 4);
    let mut buffer = Vec::new();
    buffer.resize(4, 0);
    let result = decoder.decode::<i32>(buffer.as_mut_slice(), 4);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);
  }
}
