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

use std::mem::{size_of, transmute_copy};
use std::cmp;

use errors::{Result, ParquetError};

/// Read `$size` of bytes from `$src`, and reinterpret them
/// as type `$ty`, in little-endian order. `$ty` must implement
/// the `Default` trait. Otherwise this won't compile.
/// This is copied and modified from byteorder crate.
macro_rules! read_num_bytes {
  ($ty:ty, $size:expr, $src:expr) => ({
    assert!($size <= $src.len());
    let mut data: $ty = Default::default();
    unsafe {
      ::std::ptr::copy_nonoverlapping(
        $src.as_ptr(),
        &mut data as *mut $ty as *mut u8,
        $size);
    }
    data
  });
}


/// Return the ceil of value/divisor
#[inline]
pub fn ceil(value: i64, divisor: i64) -> i64 {
  let mut result = value / divisor;
  if value % divisor != 0 { result += 1 };
  result
}

/// Return ceil(log2(x))
#[inline]
pub fn log2(mut x: u64) -> i32 {
  if x == 1 {
    return 0;
  }
  x -= 1;
  let mut result = 0;
  while x > 0 {
    x >>= 1;
    result += 1;
  }
  result
}

/// Returns the `num_bits` least-significant bits of `v`
#[inline]
pub fn trailing_bits(v: u64, num_bits: usize) -> u64 {
  if num_bits == 0 {
    return 0;
  }
  if num_bits >= 64 {
    return v;
  }
  let n = 64 - num_bits;
  (v << n) >> n
}

#[inline]
pub fn set_array_bit(bits: &mut [u8], i: usize) {
  bits[i / 8] |= 1 << (i % 8);
}

#[inline]
pub fn unset_array_bit(bits: &mut [u8], i: usize) {
  bits[i / 8] &= !(1 << (i % 8));
}


/// Maximum byte length for a VLQ encoded integer
// TODO: why maximum is 5?
const MAX_VLQ_BYTE_LEN: usize = 5;

pub struct BitReader<'a> {
  /// The byte buffer to read from, passed in by client
  buffer: &'a [u8],

  /// Bytes are memcpy'd from `buffer` and values are read from this variable.
  /// This is faster than reading values byte by byte directly from `buffer`
  buffered_values: u64,

  /// Current byte offset in `buffer`
  byte_offset: usize,

  /// Current bit offset in `buffered_values`
  bit_offset: usize,

  /// Total number of bytes in `buffer`
  total_bytes: usize
}

impl<'a> BitReader<'a> {
  pub fn new(buffer: &'a [u8]) -> Self {
    let total_bytes = buffer.len();
    let num_bytes = cmp::min(8, total_bytes);
    let buffered_values = read_num_bytes!(u64, num_bytes, buffer);
    BitReader {
      buffer: buffer, buffered_values: buffered_values,
      byte_offset: 0, bit_offset: 0, total_bytes: total_bytes
    }
  }

  #[inline]
  pub fn reset(&mut self, buffer: &'a [u8]) {
    self.buffer = buffer;
    self.total_bytes = buffer.len();
    let num_bytes = cmp::min(8, self.total_bytes);
    self.buffered_values = read_num_bytes!(u64, num_bytes, buffer);
    self.byte_offset = 0;
    self.bit_offset = 0;
  }

  /// Get the current byte offset
  #[inline]
  pub fn get_byte_offset(&self) -> usize {
    self.byte_offset + self.bit_offset / 8 + 1
  }

  #[inline]
  pub fn get_value<T: Default>(&mut self, num_bits: usize) -> Result<T> {
    assert!(num_bits <= 32);
    assert!(num_bits <= size_of::<T>() * 8);

    if self.byte_offset * 8 + self.bit_offset + num_bits > self.total_bytes * 8 {
      return general_err!("Not enough bytes left");
    }

    //
    // |...|.........|...........|
    //     ^         ^           ^
    //     num_bits  bit_offset  byte_offset
    //
    let mut v = trailing_bits(self.buffered_values, self.bit_offset + num_bits) >> self.bit_offset;
    self.bit_offset += num_bits;

    if self.bit_offset >= 64 {
      self.byte_offset += 8;
      self.bit_offset -= 64;

      let bytes_to_read = cmp::min(self.total_bytes - self.byte_offset, 8);
      self.buffered_values = read_num_bytes!(
        u64, bytes_to_read, self.buffer[self.byte_offset..]);

      v |= trailing_bits(self.buffered_values, self.bit_offset) << (num_bits - self.bit_offset);
    }

    // TODO: better to avoid copying here
    let result: T = unsafe {
      transmute_copy::<u64, T>(&v)
    };
    Ok(result)
  }

  /// Read a `num_bytes`-sized value from this buffer and return it.
  /// `T` needs to be a little-endian native type. The value is assumed to
  /// be byte aligned so the bit reader will be advanced to the start of
  /// the next byte before reading the value. Return `None` if there's not
  /// enough bytes left.
  #[inline]
  pub fn get_aligned<T: Default>(&mut self, num_bytes: usize) -> Result<T> {
    let bytes_read = ceil(self.bit_offset as i64, 8) as usize;
    if self.byte_offset + bytes_read + num_bytes > self.total_bytes {
      return general_err!("Not enough bytes left");
    }

    // Advance byte_offset to next unread byte and read num_bytes
    self.byte_offset += bytes_read;
    let v = read_num_bytes!(T, num_bytes, self.buffer[self.byte_offset..]);
    self.byte_offset += num_bytes;

    // Reset buffered_values
    self.bit_offset = 0;
    let bytes_remaining = cmp::min(self.total_bytes - self.byte_offset, 8);
    self.buffered_values = read_num_bytes!(
      u64, bytes_remaining, self.buffer[self.byte_offset..]);
    Ok(v)
  }

  /// Read a VLQ encoded (in little endian order) int from the stream.
  /// The encoded int must start at the beginning of a byte.
  /// Returns `None` if the number of bytes exceed `MAX_VLQ_BYTE_LEN`, or
  /// there's not enough bytes in the stream.
  #[inline]
  pub fn get_vlq_int(&mut self) -> Result<i64> {
    let mut shift = 0;
    let mut v: i64 = 0;
    while let Ok(byte) = self.get_aligned::<u8>(1) {
      v |= ((byte & 0x7F) as i64) << shift;
      shift += 7;
      if shift > MAX_VLQ_BYTE_LEN * 7 {
        return general_err!("Num of bytes exceed MAX_VLQ_BYTE_LEN ({})", MAX_VLQ_BYTE_LEN);
      }
      if byte & 0x80 == 0 {
        return Ok(v);
      }
    }
    general_err!("Not enough bytes left")
  }

  /// Read a zigzag-VLQ encoded (in little endian order) int from the stream
  /// Zigzag-VLQ is a variant of VLQ encoding where negative and positive
  /// numbers are encoded in a zigzag fashion.
  /// See: https://developers.google.com/protocol-buffers/docs/encoding
  ///
  /// The encoded int must start at the beginning of a byte.
  /// Returns `None` if the number of bytes exceed `MAX_VLQ_BYTE_LEN`, or
  /// there's not enough bytes in the stream.
  #[inline]
  pub fn get_zigzag_vlq_int(&mut self) -> Result<i64> {
    self.get_vlq_int().map(|v| {
      let u = v as u64;
      ((u >> 1) as i64 ^ -((u & 1) as i64))
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::error::Error;

  #[test]
  fn test_ceil() {
    assert_eq!(ceil(0, 1), 0);
    assert_eq!(ceil(1, 1), 1);
    assert_eq!(ceil(1, 2), 1);
    assert_eq!(ceil(1, 8), 1);
    assert_eq!(ceil(7, 8), 1);
    assert_eq!(ceil(8, 8), 1);
    assert_eq!(ceil(9, 8), 2);
    assert_eq!(ceil(9, 9), 1);
    assert_eq!(ceil(10000000000, 10), 1000000000);
    assert_eq!(ceil(10, 10000000000), 1);
    assert_eq!(ceil(10000000000, 1000000000), 10);
  }

  #[test]
  fn test_bit_reader_get_value() {
    let buffer = vec![255, 0];
    let mut bit_reader = BitReader::new(&buffer);
    let v1 = bit_reader.get_value::<i32>(1);
    assert!(v1.is_ok());
    assert_eq!(v1.unwrap(), 1);
    let v2 = bit_reader.get_value::<i32>(2);
    assert!(v2.is_ok());
    assert_eq!(v2.unwrap(), 3);
    let v3 = bit_reader.get_value::<i32>(3);
    assert!(v3.is_ok());
    assert_eq!(v3.unwrap(), 7);
    let v4 = bit_reader.get_value::<i32>(4);
    assert!(v4.is_ok());
    assert_eq!(v4.unwrap(), 3);
  }

  #[test]
  fn test_bit_reader_get_value_boundary() {
    let buffer = vec![10, 0, 0, 0, 20, 0, 30, 0, 0, 0, 40, 0];
    let mut bit_reader = BitReader::new(&buffer);
    let v1 = bit_reader.get_value::<i64>(32);
    assert!(v1.is_ok());
    assert_eq!(v1.unwrap(), 10);
    let v2 = bit_reader.get_value::<i16>(16);
    assert!(v2.is_ok());
    assert_eq!(v2.unwrap(), 20);
    let v3 = bit_reader.get_value::<i32>(32);
    assert!(v3.is_ok());
    assert_eq!(v3.unwrap(), 30);
    let v4 = bit_reader.get_value::<i32>(16);
    assert!(v4.is_ok());
    assert_eq!(v4.unwrap(), 40);
  }

  #[test]
  fn test_bit_reader_get_aligned() {
    // 01110101 11001011
    let buffer: Vec<u8> = vec!(0x75, 0xCB);
    let mut bit_reader = BitReader::new(&buffer);
    let v1 = bit_reader.get_value::<i32>(3);
    assert!(v1.is_ok());
    assert_eq!(v1.unwrap(), 5);
    let v2 = bit_reader.get_aligned::<i32>(1);
    assert!(v2.is_ok());
    assert_eq!(v2.unwrap(), 203);
    let v3 = bit_reader.get_value::<i32>(1);
    assert!(v3.is_err());

    bit_reader.reset(&buffer);
    let v4 = bit_reader.get_aligned::<i32>(3);
    assert!(v4.is_err());
  }

  #[test]
  fn test_bit_reader_get_vlq_int() {
    // 10001001 00000001 11110010 10110101 00000110
    let buffer: Vec<u8> = vec!(0x89, 0x01, 0xF2, 0xB5, 0x06);
    let mut bit_reader = BitReader::new(&buffer);
    let v = bit_reader.get_vlq_int();
    assert!(v.is_ok());
    assert_eq!(v.unwrap(), 137);
    let v = bit_reader.get_vlq_int();
    assert!(v.is_ok());
    assert_eq!(v.unwrap(), 105202);
  }

  #[test]
  fn test_bit_reader_get_vlq_int_overflow() {
    // 10001001 10000001 11110010 10110101 00000110
    let buffer: Vec<u8> = vec!(0x89, 0x81, 0xF2, 0xB5, 0x06);
    let mut bit_reader = BitReader::new(&buffer);
    let v = bit_reader.get_vlq_int();
    assert!(v.is_ok());
    assert_eq!(v.unwrap(), 1723629705);


    // 10001001 10000001 11110010 10110101 10000110 00000001
    let buffer = vec!(0x89, 0x81, 0xF2, 0xB5, 0x86, 0x01);
    let mut bit_reader = BitReader::new(&buffer);
    let v = bit_reader.get_vlq_int();
    assert!(v.is_err());
    assert_eq!(v.unwrap_err().description(),
      format!("Num of bytes exceed MAX_VLQ_BYTE_LEN ({})", MAX_VLQ_BYTE_LEN));
  }

  #[test]
  fn test_bit_reader_get_zigzag_vlq_int() {
    let buffer: Vec<u8> = vec!(0, 1, 2, 3);
    let mut bit_reader = BitReader::new(&buffer);

    let v = bit_reader.get_zigzag_vlq_int();
    assert!(v.is_ok());
    assert_eq!(v.unwrap(), 0);

    let v = bit_reader.get_zigzag_vlq_int();
    assert!(v.is_ok());
    assert_eq!(v.unwrap(), -1);

    let v = bit_reader.get_zigzag_vlq_int();
    assert!(v.is_ok());
    assert_eq!(v.unwrap(), 1);

    let v = bit_reader.get_zigzag_vlq_int();
    assert!(v.is_ok());
    assert_eq!(v.unwrap(), -2);
  }

  #[test]
  fn test_set_array_bit() {
    let mut buffer = vec![0, 0, 0];
    set_array_bit(&mut buffer[..], 1);
    assert_eq!(buffer, vec![2, 0, 0]);
    set_array_bit(&mut buffer[..], 4);
    assert_eq!(buffer, vec![18, 0, 0]);
    unset_array_bit(&mut buffer[..], 1);
    assert_eq!(buffer, vec![16, 0, 0]);
    set_array_bit(&mut buffer[..], 10);
    assert_eq!(buffer, vec![16, 4, 0]);
    set_array_bit(&mut buffer[..], 10);
    assert_eq!(buffer, vec![16, 4, 0]);
    set_array_bit(&mut buffer[..], 11);
    assert_eq!(buffer, vec![16, 12, 0]);
    unset_array_bit(&mut buffer[..], 10);
    assert_eq!(buffer, vec![16, 8, 0]);
  }

  #[test]
  fn test_log2() {
    assert_eq!(log2(1), 0);
    assert_eq!(log2(2), 1);
    assert_eq!(log2(3), 2);
    assert_eq!(log2(4), 2);
    assert_eq!(log2(5), 3);
    assert_eq!(log2(5), 3);
    assert_eq!(log2(6), 3);
    assert_eq!(log2(7), 3);
    assert_eq!(log2(8), 3);
    assert_eq!(log2(9), 4);
  }

}
