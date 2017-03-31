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
use errors::{Result, ParquetError};
use util::bit_util::BitReader;
use util::memory::{Buffer, ByteBuffer, MutableBuffer};
use super::rle_encoding::RleDecoder;

// ----------------------------------------------------------------------
// Decoders

pub trait Decoder<'a, T: DataType<'a>> {
  /// Set the data to decode to be `data`, which should contain `num_values` of
  /// values to decode
  fn set_data(&mut self, data: &'a [u8], num_values: usize) -> Result<()>;

  /// Try to consume at most `max_values` from this decoder and write
  /// the result to `buffer`.. Return the actual number of values written.
  /// N.B., `buffer.len()` must at least be `max_values`.
  fn decode(&mut self, buffer: &mut [T::T], max_values: usize) -> Result<usize>;

  /// Number of values left in this decoder stream
  fn values_left(&self) -> usize;

  /// Return the encoding for this decoder
  fn encoding(&self) -> Encoding;
}


// ----------------------------------------------------------------------
// PLAIN Decoding

pub struct PlainDecoder<'a, T: DataType<'a>> {
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
  pub fn new() -> Self {
    PlainDecoder {
      data: None, bit_reader: None,
      num_values: 0, start: 0, _phantom: PhantomData
    }
  }
}

impl<'a, T: DataType<'a>> Decoder<'a, T> for PlainDecoder<'a, T> {
  #[inline]
  default fn set_data(&mut self, data: &'a [u8], num_values: usize) -> Result<()> {
    self.num_values = num_values;
    self.data = Some(data);
    Ok(())
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
      return general_err!("Not enough bytes to decode");
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

impl<'a> Decoder<'a, Int96Type> for PlainDecoder<'a, Int96Type> {
  fn decode(&mut self, buffer: &mut [Int96<'a>], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = 12 * num_values;
    if bytes_left < bytes_to_decode {
      return general_err!("Not enough bytes to decode");
    }
    for i in 0..num_values {
      buffer[i].set_data(
        unsafe {
          let data_slice = &data[self.start..self.start + 12];
          ::std::slice::from_raw_parts(data_slice.as_ptr() as *const u32, 3)
        }
      );
      self.start += 12;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl<'a> Decoder<'a, BoolType> for PlainDecoder<'a, BoolType> {
  fn set_data(&mut self, data: &'a [u8], num_values: usize) -> Result<()> {
    self.num_values = num_values;
    self.bit_reader = Some(BitReader::new(data));
    Ok(())
  }

  fn decode(&mut self, buffer: &mut [bool], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.bit_reader.is_some());

    let mut bit_reader = self.bit_reader.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    for i in 0..num_values {
      bit_reader.get_value::<bool>(1).map(|b| {
        buffer[i] = b;
      })?;
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
      let len: usize = read_num_bytes!(u32, 4, &data[self.start..]) as usize;
      self.start += mem::size_of::<u32>();
      if data.len() < self.start + len {
        return general_err!("Not enough bytes to decode");
      }
      buffer[i].set_data(&data[self.start..self.start + len]);
      self.start += len;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl<'a> Decoder<'a, FixedLenByteArrayType> for PlainDecoder<'a, FixedLenByteArrayType> {
  fn decode(&mut self, buffer: &mut [FixedLenByteArray<'a>], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    let type_length = buffer[0].get_len();
    for i in 0..num_values {
      if data.len() < self.start + type_length {
        return general_err!("Not enough bytes to decode");
      }
      buffer[i].set_data(&data[self.start..self.start + type_length]);
      self.start += type_length;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}


// ----------------------------------------------------------------------
// PLAIN_DICTIONARY Decoding

pub struct DictDecoder<'a, T: DataType<'a>> {
  /// The dictionary, which maps ids to the values
  dictionary: Vec<T::T>,

  /// The decoder for the value ids
  rle_decoder: Option<RleDecoder<'a>>,

  /// Number of values left in the data stream
  num_values: usize
}

impl<'a, T: DataType<'a>> DictDecoder<'a, T> {
  pub fn new(dict: Vec<T::T>) -> Self {
    Self { dictionary: dict, rle_decoder: None, num_values: 0 }
  }
}

impl<'a, T: DataType<'a>> Decoder<'a, T> for DictDecoder<'a, T> {
  fn set_data(&mut self, data: &'a [u8], num_values: usize) -> Result<()> {
    // first byte in `data` is bit width
    let bit_width = data[0] as usize;
    let mut rle_decoder = RleDecoder::new(bit_width);
    rle_decoder.set_data(&data[1..]);
    self.num_values = num_values;
    self.rle_decoder = Some(rle_decoder);
    Ok(())
  }

  fn decode(&mut self, buffer: &mut [T::T], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.rle_decoder.is_some());

    let mut rle = self.rle_decoder.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    rle.decode_with_dict(&self.dictionary[..], buffer, num_values)
  }

  /// Number of values left in this decoder stream
  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::PLAIN_DICTIONARY
  }

}


// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED Decoding

pub struct DeltaBitPackDecoder<'a, T: DataType<'a>> {
  bit_reader: Option<BitReader<'a>>,

  // Header info
  num_values: usize,
  num_mini_blocks: i64,
  values_per_mini_block: i64,
  values_current_mini_block: i64,
  first_value: i64,
  first_value_read: bool,

  // Per block info
  min_delta: i64,
  mini_block_idx: usize,
  delta_bit_width: u8,
  delta_bit_widths: ByteBuffer,

  current_value: i64,

  _phantom: PhantomData<T>
}

impl<'a, T: DataType<'a>> DeltaBitPackDecoder<'a, T> {
  pub fn new() -> Self {
    Self { bit_reader: None, num_values: 0, num_mini_blocks: 0,
           values_per_mini_block: 0, values_current_mini_block: 0,
           first_value: 0, first_value_read: false,
           min_delta: 0, mini_block_idx: 0, delta_bit_width: 0,
           delta_bit_widths: ByteBuffer::new(0),
           current_value: 0, _phantom: PhantomData }
  }

  pub fn get_offset(&self) -> usize {
    assert!(self.bit_reader.is_some());
    let reader = self.bit_reader.as_ref().unwrap();
    reader.get_byte_offset()
  }

  #[inline]
  fn init_block(&mut self) -> Result<()> {
    assert!(self.bit_reader.is_some());
    let mut bit_reader = self.bit_reader.as_mut().unwrap();

    self.min_delta = bit_reader.get_zigzag_vlq_int()?;
    let mut widths = vec!();
    for _ in 0..self.num_mini_blocks {
      let w = bit_reader.get_aligned::<u8>(1)?;
      widths.push(w);
    }
    self.delta_bit_widths.set_data(widths);
    self.mini_block_idx = 0;
    self.values_current_mini_block = self.values_per_mini_block;
    self.current_value = self.first_value; // TODO: double check this
    Ok(())
  }
}

impl<'a> Decoder<'a, Int64Type> for DeltaBitPackDecoder<'a, Int64Type> {
  // # of total values is derived from encoding
  fn set_data(&mut self, data: &'a [u8], _: usize) -> Result<()> {
    let mut bit_reader = BitReader::new(data);

    let block_size = bit_reader.get_vlq_int()?;
    self.num_mini_blocks = bit_reader.get_vlq_int()?;
    self.num_values = bit_reader.get_vlq_int()? as usize;
    self.first_value = bit_reader.get_zigzag_vlq_int()?;
    self.first_value_read = false;
    self.values_per_mini_block = (block_size / self.num_mini_blocks) as i64;
    assert!(self.values_per_mini_block % 8 == 0);

    self.bit_reader = Some(bit_reader);
    Ok(())
  }

  // TODO: same impl for i32?
  fn decode(&mut self, buffer: &mut [i64], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.bit_reader.is_some());

    let num_values = cmp::min(max_values, self.num_values);
    for i in 0..num_values {
      if !self.first_value_read {
        buffer[i] = self.first_value;
        self.first_value_read = true;
        continue;
      }

      if self.values_current_mini_block == 0 {
        self.mini_block_idx += 1;
        if self.mini_block_idx < self.delta_bit_widths.size() {
          self.delta_bit_width = self.delta_bit_widths.data()[self.mini_block_idx];
          self.values_current_mini_block = self.values_per_mini_block;
        } else {
          self.init_block()?;
        }
      }

      // we can't move this outside the loop since it needs to
      // "carve out" the permission of `self.bit_reader` which makes
      // `self.init_block()` call impossible (the latter requires the
      // whole permission of `self`)
      // TODO: evaluate the performance impact of this.
      let bit_reader = self.bit_reader.as_mut().unwrap();

      // TODO: use SIMD to optimize this?
      let delta = bit_reader.get_value(self.delta_bit_width as usize)?;
      self.current_value += self.min_delta;
      self.current_value += delta;
      buffer[i] = self.current_value;
      self.values_current_mini_block -= 1;
    }

    self.num_values -= num_values;
    Ok(num_values)
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_BINARY_PACKED
  }

}

// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY Decoding

pub struct DeltaLengthByteArrayDecoder<'a, T: DataType<'a>> {
  // Lengths for each byte array in `data`
  // TODO: add memory tracker to this
  lengths: Vec<i64>,

  // Current index into `lengths`
  current_idx: usize,

  // Concatenated byte array data
  data: Option<&'a [u8]>,

  // Offset into `data`, always point to the beginning of next byte array.
  offset: usize,

  num_values: usize,
  _phantom: PhantomData<T>
}

impl<'a, T: DataType<'a>> DeltaLengthByteArrayDecoder<'a, T> {
  pub fn new() -> Self {
    Self { lengths: vec!(), current_idx: 0, data: None, offset: 0,
           num_values: 0, _phantom: PhantomData }
  }
}

impl<'a> Decoder<'a, ByteArrayType> for DeltaLengthByteArrayDecoder<'a, ByteArrayType> {
  fn set_data(&mut self, data: &'a [u8], num_values: usize) -> Result<()> {
    let mut len_decoder = DeltaBitPackDecoder::new();
    len_decoder.set_data(data, num_values)?;
    let num_lengths = len_decoder.values_left();
    self.lengths.resize(num_lengths, 0);
    len_decoder.decode(&mut self.lengths[..], num_lengths)?;

    self.data = Some(&data[len_decoder.get_offset()..]);
    self.offset = 0;
    self.num_values = num_lengths;
    self.current_idx = 0;
    Ok(())
  }

  fn decode(&mut self, buffer: &mut [ByteArray<'a>], max_values: usize) -> Result<usize> {
    assert!(self.data.is_some());
    assert!(buffer.len() >= max_values);

    let data = self.data.as_ref().unwrap();
    let num_values = cmp::min(self.num_values, max_values);
    for i in 0..num_values {
      let len = self.lengths[self.current_idx] as usize;
      buffer[i].set_data(&data[self.offset..self.offset + len]);
      self.offset += len;
      self.current_idx += 1;
    }

    self.num_values -= num_values;
    Ok(num_values)
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_LENGTH_BYTE_ARRAY
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use std::mem;
  use util::bit_util::set_array_bit;

  #[test]
  fn test_plain_decode_int32() {
    let data = vec![42, 18, 52];
    let data_bytes = <i32 as ToByteArray<i32>>::to_byte_array(&data[..]);
    let mut buffer = vec![0; 3];
    test_plain_decode::<Int32Type>(&data_bytes[..], 3, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_int64() {
    let data = vec![42, 18, 52];
    let data_bytes = <i64 as ToByteArray<i64>>::to_byte_array(&data[..]);
    let mut buffer = vec![0; 3];
    test_plain_decode::<Int64Type>(&data_bytes[..], 3, &mut buffer[..], &data[..]);
  }


  #[test]
  fn test_plain_decode_float() {
    let data = vec![3.14, 2.414, 12.51];
    let data_bytes = <f32 as ToByteArray<f32>>::to_byte_array(&data[..]);
    let mut buffer = vec![0.0; 3];
    test_plain_decode::<FloatType>(&data_bytes[..], 3, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_double() {
    let data = vec![3.14f64, 2.414f64, 12.51f64];
    let data_bytes = <f64 as ToByteArray<f64>>::to_byte_array(&data[..]);
    let mut buffer = vec![0.0f64; 3];
    test_plain_decode::<DoubleType>(&data_bytes[..], 3, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_int96() {
    let v0 = [11, 22, 33];
    let v1 = [44, 55, 66];
    let v2 = [10, 20, 30];
    let v3 = [40, 50, 60];
    let mut data = vec![Int96::new(); 4];
    data[0].set_data(&v0);
    data[1].set_data(&v1);
    data[2].set_data(&v2);
    data[3].set_data(&v3);
    let data_bytes = <Int96 as ToByteArray<Int96>>::to_byte_array(&data[..]);
    let mut buffer = vec![Int96::new(); 4];
    test_plain_decode::<Int96Type>(&data_bytes[..], 4, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_bool() {
    let data = vec![false, true, false, false, true, false, true, true, false, true];
    let data_bytes = <bool as ToByteArray<bool>>::to_byte_array(&data[..]);
    let mut buffer = vec![false; 10];
    test_plain_decode::<BoolType>(&data_bytes[..], 10, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_byte_array() {
    let mut data = vec!(ByteArray::new(); 2);
    data[0].set_data("hello".as_bytes());
    data[1].set_data("parquet".as_bytes());
    let data_bytes = <ByteArray as ToByteArray<ByteArray>>::to_byte_array(&data[..]);
    let mut buffer = vec![ByteArray::new(); 2];
    test_plain_decode::<ByteArrayType>(&data_bytes[..], 2, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_fixed_len_byte_array() {
    let mut data = vec!(FixedLenByteArray::new(4); 3);
    data[0].set_data("bird".as_bytes());
    data[1].set_data("come".as_bytes());
    data[2].set_data("flow".as_bytes());
    let data_bytes = <FixedLenByteArray as ToByteArray<FixedLenByteArray>>::to_byte_array(&data[..]);
    let mut buffer = vec![FixedLenByteArray::new(4); 3];
    test_plain_decode::<FixedLenByteArrayType>(&data_bytes[..], 3, &mut buffer[..], &data[..]);
  }


  fn test_plain_decode<'a, T: DataType<'a>>(data: &'a [u8], num_values: usize,
                                            buffer: &mut [T::T], expected: &[T::T]) {
    let mut decoder: PlainDecoder<T> = PlainDecoder::new();
    let result = decoder.set_data(&data[..], num_values);
    assert!(result.is_ok());
    let result = decoder.decode(&mut buffer[..], num_values);
    assert!(result.is_ok());
    assert_eq!(decoder.values_left(), 0);
    assert_eq!(buffer, expected);
  }


  fn usize_to_bytes<'a>(v: usize) -> [u8; 4] {
    unsafe { mem::transmute::<u32, [u8; 4]>(v as u32) }
  }

  /// A util trait to convert slices of different types to byte arrays
  trait ToByteArray<T> {
    fn to_byte_array(data: &[T]) -> Vec<u8>;
  }

  impl<T> ToByteArray<T> for T {
    default fn to_byte_array(data: &[T]) -> Vec<u8> {
      let mut v = vec!();
      let type_len = ::std::mem::size_of::<T>();
      v.extend_from_slice(
        unsafe {
          ::std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * type_len)
        }
      );
      v
    }
  }

  impl ToByteArray<bool> for bool {
    fn to_byte_array(data: &[bool]) -> Vec<u8> {
      let mut v = vec!();
      for i in 0..data.len() {
        if i % 8 == 0 {
          v.push(0);
        }
        if data[i] {
          set_array_bit(&mut v[..], i);
        }
      }
      v
    }
  }

  impl<'a> ToByteArray<Int96<'a>> for Int96<'a> {
    fn to_byte_array(data: &[Int96<'a>]) -> Vec<u8> {
      let mut v = vec!();
      for d in data {
        v.extend_from_slice(
          unsafe {
            ::std::slice::from_raw_parts(d.get_data().as_ptr() as *const u8, 12)
          }
        );
      }
      v
    }
  }

  impl<'a> ToByteArray<ByteArray<'a>> for ByteArray<'a> {
    fn to_byte_array(data: &[ByteArray<'a>]) -> Vec<u8> {
      let mut v = vec!();
      for d in data {
        let buf = d.get_data();
        v.extend_from_slice(&usize_to_bytes(buf.len()));
        v.extend_from_slice(
          unsafe {
            ::std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len())
          }
        );
      }
      v
    }
  }

  impl<'a> ToByteArray<FixedLenByteArray<'a>> for FixedLenByteArray<'a> {
    fn to_byte_array(data: &[FixedLenByteArray<'a>]) -> Vec<u8> {
      let mut v = vec!();
      for d in data {
        let buf = d.get_data();
        v.extend_from_slice(
          unsafe {
            ::std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len())
          }
        );
      }
      v
    }
  }

}
