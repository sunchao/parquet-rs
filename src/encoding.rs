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
use util::bit_util::{self, BitReader};

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

  /// Return the encoding for this decoder
  fn encoding(&self) -> Encoding;
}


// ----------------------------------------------------------------------
// Plain Decoders

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

  #[inline]
  fn values_left(&self) -> usize {
    self.num_values
  }
}

impl<'a, T: DataType<'a>> Decoder<'a, T> for PlainDecoder<'a, T> {
  #[inline]
  default fn set_data(&mut self, data: &'a [u8], num_values: usize) {
    self.num_values = num_values;
    self.data = Some(data);
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

impl<'a> Decoder<'a, Int96Type> for PlainDecoder<'a, Int96Type> {
  fn decode(&mut self, buffer: &mut [Int96<'a>], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = 12 * num_values;
    if bytes_left < bytes_to_decode {
      return Err(decode_err!("Not enough bytes to decode"));
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
      if let Some(b) = bit_reader.get_value::<bool>(1) {
        buffer[i] = b;
      } else {
        return Err(decode_err!("Cannot decode bool"));
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
      let len: usize = read_num_bytes!(u32, 4, &data[self.start..]) as usize;
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

impl<'a> Decoder<'a, FixedLenByteArrayType> for PlainDecoder<'a, FixedLenByteArrayType> {
  fn decode(&mut self, buffer: &mut [FixedLenByteArray<'a>], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(max_values, self.num_values);
    let type_length = buffer[0].get_len();
    for i in 0..num_values {
      if data.len() < self.start + type_length {
        return Err(decode_err!("Not enough bytes to decode"));
      }
      buffer[i].set_data(&data[self.start..self.start + type_length]);
      self.start += type_length;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}


// ----------------------------------------------------------------------
// RLE/Bit-Packing Hybrid Decoders

pub struct RleDecoder<'a, T: DataType<'a>> {
  /// Number of bits used to encode the value
  bit_width: usize,

  /// Bit reader loaded with input buffer.
  bit_reader: Option<BitReader<'a>>,

  /// The remaining number of values in RLE for this run
  rle_left: i64,

  /// The remaining number of values in Bit-Packing for this run
  bit_packing_left: i64,

  /// The current value for the case of RLE mode
  current_value: Option<T::T>,

  /// To allow `T` in the generic parameter for this struct. This doesn't take any space.
  _phantom: PhantomData<T>
}

impl<'a, T: DataType<'a>> RleDecoder<'a, T> {
  pub fn new(bit_width: usize) -> Self {
    RleDecoder { bit_width: bit_width, rle_left: 0, bit_packing_left: 0,
                 bit_reader: None, current_value: None, _phantom: PhantomData }
  }

  fn reload(&mut self) -> bool {
    assert!(self.bit_reader.is_some());
    if let Some(ref mut bit_reader) = self.bit_reader {
      if let Some(indicator_value) = bit_reader.get_vlq_int() {
        if indicator_value & 1 == 1 {
          self.bit_packing_left = (indicator_value >> 1) * 8;
        } else {
          self.rle_left = indicator_value >> 1;
          let value_width = bit_util::ceil(self.bit_width as i64, 8);
          self.current_value = bit_reader.get_aligned::<T::T>(value_width as usize);
          assert!(self.current_value.is_some());
        }
        return true;
      }
    }
    return false;
  }
}

impl<'a, T: DataType<'a>> Decoder<'a, T> for RleDecoder<'a, T> {
  /// For RleDecoder, the `num_values` is not used.
  fn set_data(&mut self, data: &'a [u8], _: usize) {
    if let Some(ref mut bit_reader) = self.bit_reader {
      bit_reader.reset(data);
    } else {
      self.bit_reader = Some(BitReader::new(data));
    }

    let _ = self.reload();
  }

  fn decode(&mut self, buffer: &mut [T::T], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.bit_reader.is_some());

    let mut values_read = 0;
    while values_read < max_values {
      if self.rle_left > 0 {
        assert!(self.current_value.is_some());
        let repeated_value = self.current_value.as_mut().unwrap();
        let num_values = cmp::min(max_values - values_read, self.rle_left as usize);
        for i in 0..num_values {
          buffer[values_read + i] = repeated_value.clone();
        }
        self.rle_left -= num_values as i64;
        values_read += num_values;
      } else if self.bit_packing_left > 0 {
        let num_values = cmp::min(max_values - values_read, self.bit_packing_left as usize);
        if let Some(ref mut bit_reader) = self.bit_reader {
          for i in 0..num_values {
            if let Some(v) = bit_reader.get_value(self.bit_width) {
              buffer[values_read + i] = v;
            } else {
              return Err(decode_err!("Error when reading bit-packed value"));
            }
          }
          self.bit_packing_left -= num_values as i64;
          values_read += num_values;
        } else {
          return Err(decode_err!("Bit reader should not be None"));
        }
      } else {
        if !self.reload() {
          break;
        }
      }
    }

    Ok(values_read)
  }

  fn encoding(&self) -> Encoding {
    Encoding::RLE
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
    decoder.set_data(&data[..], num_values);
    let result = decoder.decode(&mut buffer[..], num_values);
    assert!(result.is_ok());
    assert_eq!(decoder.values_left(), 0);
    assert_eq!(buffer, expected);
  }

  #[test]
  fn test_rle_decode_int32() {
    // test data: 0-7 with bit width 3
    // 00000011 10001000 11000110 11111010
    let data = vec!(0x03, 0x88, 0xC6, 0xFA);
    let mut decoder: RleDecoder<Int32Type> = RleDecoder::new(3);
    decoder.set_data(&data, 0);
    let mut buffer = vec!(0; 8);
    let expected = vec!(0, 1, 2, 3, 4, 5, 6, 7);
    let result = decoder.decode(&mut buffer, 8);
    assert!(result.is_ok());
    assert_eq!(buffer, expected);
  }

  #[test]
  fn test_rle_decode_bool() {
    // rle test data: 50 1s followed by 50 0s
    // 01100100 00000001 01100100 00000000
    let data1 = vec!(0x64, 0x01, 0x64, 0x00);

    // bit-packing test data: alternating 1s and 0s, 100 total
    // 100 / 8 = 13 groups
    // 00011011 10101010 ... 00001010
    let data2 = vec!(0x1B, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA,
                     0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0xAA, 0x0A);

    let mut decoder: RleDecoder<BoolType> = RleDecoder::new(1);
    decoder.set_data(&data1, 0);
    let mut buffer = vec!(false; 100);
    let mut expected = vec!();
    for i in 0..100 {
      if i < 50 {
        expected.push(true);
      } else {
        expected.push(false);
      }
    }
    let result = decoder.decode(&mut buffer, 100);
    assert!(result.is_ok());
    assert_eq!(buffer, expected);

    decoder.set_data(&data2, 0);
    let mut buffer = vec!(false; 100);
    let mut expected = vec!();
    for i in 0..100 {
      if i % 2 == 0 {
        expected.push(false);
      } else {
        expected.push(true);
      }
    }
    let result = decoder.decode(&mut buffer, 100);
    assert!(result.is_ok());
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
