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
use std::marker::PhantomData;
use std::slice::from_raw_parts_mut;
use basic::*;
use basic::{Type as PhysicalType};
use errors::{Result, ParquetError};
use file::metadata::ColumnPath;
use schema::types::ColumnDescriptor;
use util::bit_util::BitReader;

// ----------------------------------------------------------------------
// Decoders

pub trait Decoder<'a, T: DataType> {
  /// Set the data to decode to be `data`, which should contain `num_values` of
  /// values to decode
  fn set_data(&mut self, data: &'a mut [u8], num_values: usize);

  /// Try to consume at most `max_values` from this decoder and write
  /// the result to `buffer`. Return the actual number of values written.
  /// N.B., `buffer` should have at least `max_values` capacity.
  fn decode(&mut self, buffer: &mut [T::T], max_values: usize) -> Result<usize>;

  /// Return the number of values left in the current buffer to decode
  fn values_left(&self) -> usize;

  /// Return the encoding for this decoder
  fn encoding(&self) -> Encoding;
}

pub struct PlainDecoder<'a, T: DataType> {
  /// A column descriptor about the primitive type this decoder is for
  descriptor: ColumnDescriptor,

  /// The remaining number of values in the byte array
  num_values: usize,

  /// The current starting index in the byte array.
  start: usize,

  /// The byte array to decode from. Not set if `T` is bool.
  data: Option<&'a mut[u8]>,

  /// Read `data` bit by bit. Only set if `T` is bool.
  bit_reader: Option<BitReader<'a>>,

  /// To allow `T` in the generic parameter for this struct. This doesn't take any space.
  _phantom: PhantomData<T>
}

impl<'a, T: DataType> PlainDecoder<'a, T> {
  pub fn new(desc: ColumnDescriptor) -> Self {
    PlainDecoder {
      descriptor: desc, data: None, bit_reader: None,
      num_values: 0, start: 0, _phantom: PhantomData
    }
  }
}

impl<'a, T: DataType> Decoder<'a, T> for PlainDecoder<'a, T> {
  #[inline]
  default fn set_data(&mut self, data: &'a mut[u8], num_values: usize) {
    self.num_values = num_values;
    self.data = Some(data);
  }

  #[inline]
  default fn values_left(&self) -> usize {
    self.num_values
  }

  #[inline]
  default fn encoding(&self) -> Encoding {
    Encoding::PLAIN
  }

  #[inline]
  default fn decode(
    &mut self, buffer: &mut [T::T], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.data.is_some());
    let mut data = self.data.as_mut().unwrap();
    let type_length = mem::size_of::<T::T>();
    let num_values = cmp::min(max_values, self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = type_length * num_values;
    if bytes_left < bytes_to_decode {
      return Err(decode_err!("Not enough bytes to decode"));
    }
    let raw_buffer: &mut [u8] = unsafe {
      from_raw_parts_mut(buffer.as_ptr() as *mut u8, bytes_to_decode)
    };
    raw_buffer.copy_from_slice(&data[self.start..self.start + bytes_to_decode]);
    self.start += bytes_to_decode;
    Ok(num_values)
  }
}

impl<'a> Decoder<'a, BoolType> for PlainDecoder<'a, BoolType> {
  fn set_data(&mut self, data: &'a mut[u8], num_values: usize) {
    self.num_values = num_values;
    self.bit_reader = Some(BitReader::new(data));
  }

  fn decode(&mut self, buffer: &mut [bool], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.bit_reader.is_some());
    let mut bit_reader = self.bit_reader.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    for i in 0..num_values {
      match bit_reader.get_value::<bool>(1) {
        Some(b) => buffer[i] = b,
        None => {
          return Err(decode_err!("Cannot decode bool"));
        }
      }
    }
    self.num_values -= num_values;
    Ok(num_values)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use schema::types::PrimitiveType;

  #[test]
  fn test_decode() {
    let pty = PrimitiveType::new(
      "foo", Repetition::OPTIONAL, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, Some(0)).unwrap();
    let desc = ColumnDescriptor::new(ColumnPath::new(vec![String::from("foo")]), pty, 0, 0);
    let mut data = vec![42, 0, 0, 0, 18, 0, 0, 0, 52, 0, 0, 0];
    let mut decoder: PlainDecoder<Int32Type> = PlainDecoder::new(desc);
    decoder.set_data(&mut data[..], 3);
    let mut buffer = vec![0; 4];
    let result = decoder.decode(buffer.as_mut_slice(), 4);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);
  }

  #[test]
  fn test_decode_bool() {
    let pty = PrimitiveType::new(
      "foo", Repetition::OPTIONAL, PhysicalType::BOOLEAN,
      LogicalType::NONE, 0, 0, 0, Some(0)).unwrap();
    let desc = ColumnDescriptor::new(ColumnPath::new(vec![String::from("foo")]), pty, 0, 0);
    let mut data = vec![202];
    let mut decoder: PlainDecoder<BoolType> = PlainDecoder::new(desc);
    decoder.set_data(&mut data[..], 8);
    let mut buffer = vec![false; 8];
    let result = decoder.decode(buffer.as_mut_slice(), 8);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 8);
    assert_eq!(buffer, vec![false, true, false, true, false, false, true, true]);
  }
}
