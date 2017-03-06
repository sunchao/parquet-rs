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
use basic::Type as PhysicalType;
use errors::{Result, ParquetError};
use file::metadata::ColumnPath;
use schema::types::ColumnDescriptor;
use util::bit_util::BitReader;

// ----------------------------------------------------------------------
// Decoders

pub trait Decoder<'a, T: DataType<'a>> {
  /// Set the data to decode to be `data`, which should contain `num_values` of
  /// values to decode
  fn set_data(&mut self, data: &'a [u8], num_values: usize);

  /// Try to consume at most `max_values` from this decoder and write
  /// the result to `buffer`.. Return the actual number of values written.
  /// N.B., `buffer.len()` must at least be `max_values`.
  fn decode(&mut self, buffer: &mut [T::T], max_values: usize) -> Result<usize>;

  /// Return the number of values left in the current buffer to decode
  fn values_left(&self) -> usize;

  /// Return the encoding for this decoder
  fn encoding(&self) -> Encoding;
}

pub struct PlainDecoder<'a, T: DataType<'a>> {
  /// A column descriptor about the primitive type this decoder is for
  descriptor: ColumnDescriptor,

  /// The remaining number of values in the byte array
  num_values: usize,

  /// The current starting index in the byte array.
  start: usize,

  /// The byte array to decode from. Not set if `T` is bool.
  data: Option<&'a [u8]>,

  /// Read `data` bit by bit. Only set if `T` is bool.
  bit_reader: Option<BitReader<'a>>,

  /// To allow `T` in the generic parameter for this struct. This doesn't take any space.
  _phantom: PhantomData<T>
}

impl<'a, T: DataType<'a>> PlainDecoder<'a, T> {
  pub fn new(desc: ColumnDescriptor) -> Self {
    PlainDecoder {
      descriptor: desc, data: None, bit_reader: None,
      num_values: 0, start: 0, _phantom: PhantomData
    }
  }
}

impl<'a, T: DataType<'a>> Decoder<'a, T> for PlainDecoder<'a, T> {
  #[inline]
  default fn set_data(&mut self, data: &'a [u8], num_values: usize) {
    self.num_values = num_values;
    self.data = Some(data);
  }

  #[inline]
  fn values_left(&self) -> usize {
    self.num_values
  }

  #[inline]
  fn encoding(&self) -> Encoding {
    Encoding::PLAIN
  }

  #[inline]
  default fn decode(&mut self, buffer: &mut [T::T], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.data.is_some());
    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = mem::size_of::<T::T>() * num_values;
    if bytes_left < bytes_to_decode {
      return Err(decode_err!("Not enough bytes to decode"));
    }
    let raw_buffer: &mut [u8] = unsafe {
      from_raw_parts_mut(buffer.as_ptr() as *mut u8, bytes_to_decode)
    };
    raw_buffer.copy_from_slice(&data[self.start..self.start + bytes_to_decode]);
    self.start += bytes_to_decode;
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl<'a> Decoder<'a, BoolType> for PlainDecoder<'a, BoolType> {
  fn set_data(&mut self, data: &'a [u8], num_values: usize) {
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

impl<'a> Decoder<'a, ByteArrayType> for PlainDecoder<'a, ByteArrayType> {
  fn decode(&mut self, buffer: &mut [ByteArray<'a>], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    for i in 0..num_values {
      let len: usize = read_num_bytes!(u32, 4, &data[self.start..], to_le) as usize;
      self.start += mem::size_of::<u32>();
      if data.len() < self.start + len {
        return Err(decode_err!("Not enough bytes to decode"));
      }
      buffer[i].set_data(&data[self.start..self.start + len]);
      self.start += len;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use schema::types::PrimitiveType;
  use std::mem;

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
    let mut result = decoder.decode(&mut buffer[..], 2);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);
    assert_eq!(buffer, vec![42, 18, 0, 0]);
    assert_eq!(decoder.values_left(), 1);

    result = decoder.decode(&mut buffer[2..], 2);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
    assert_eq!(buffer, vec![42, 18, 52, 0]);
    assert_eq!(decoder.values_left(), 0);
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
    let result = decoder.decode(&mut buffer, 8);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 8);
    assert_eq!(buffer, vec![false, true, false, true, false, false, true, true]);
    assert_eq!(decoder.values_left(), 0);
  }

  #[test]
  fn test_decode_byte_array() {
    let pty = PrimitiveType::new(
      "foo", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
      LogicalType::NONE, 0, 0, 0, Some(0)).unwrap();
    let desc = ColumnDescriptor::new(ColumnPath::new(vec![String::from("foo")]), pty, 0, 0);
    let mut data: Vec<u8> = vec!();
    let mut decoder: PlainDecoder<ByteArrayType> = PlainDecoder::new(desc);

    let s1 = "hello";
    let s2 = "you";
    let len_a = unsafe { mem::transmute::<u32, [u8; 4]>(s1.len() as u32) };
    let len_b = unsafe { mem::transmute::<u32, [u8; 4]>(s2.len() as u32) };
    data.extend(len_a.iter());
    data.extend(s1.as_bytes().iter());
    data.extend(len_b.iter());
    data.extend(s2.as_bytes().iter());
    decoder.set_data(&mut data[..], 2);

    let mut buffer = vec![ByteArray::new(); 2];
    let result = decoder.decode(&mut buffer[..], 2);
    assert!(result.is_ok());
  }
}
