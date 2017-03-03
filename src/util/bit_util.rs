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
use std::ptr::copy_nonoverlapping;

macro_rules! read_num_bytes {
  ($ty:ty, $size:expr, $src:expr, $which:ident) => ({
    assert!($size <= $src.len());
    let mut data: $ty = 0;
    unsafe {
      copy_nonoverlapping(
        $src.as_ptr(),
        &mut data as *mut $ty as *mut u8,
        $size);
    }
    data.$which()
  });
}


#[inline]
fn trailing_bits(v: u64, num_bits: usize) -> u64 {
  if num_bits == 0 {
    return 0;
  }
  if num_bits >= 64 {
    return v;
  }
  let n = 64 - num_bits;
  (v << n) >> n
}

pub struct BitReader<'a> {
  /// The buffer to read from, passed in by client
  buffer: &'a [u8],

  /// Bytes are memcpy'd from buffer_ and values are read from this variable. This is
  /// faster than reading values byte by byte directly from buffer_.
  buffered_values: u64,

  /// Byte offset in `buffer`
  byte_offset: usize,

  /// Bit offset in `buffered_values`
  bit_offset: usize,

  /// Total number of bytes in `buffer`
  total_bytes: usize
}

impl<'a> BitReader<'a> {
  pub fn new(buffer: &'a mut [u8]) -> Self {
    let total_bytes = buffer.len();
    let num_bytes = cmp::min(8, total_bytes);
    let buffered_values = read_num_bytes!(u64, num_bytes, buffer, to_le);
    BitReader {
      buffer: buffer, buffered_values: buffered_values,
      byte_offset: 0, bit_offset: 0, total_bytes: total_bytes
    }
  }

  #[inline]
  pub fn reset(&mut self, buffer: &'a [u8]) {
    self.buffer = buffer;
    self.byte_offset = 0;
    self.bit_offset = 0;
  }


  #[inline]
  pub fn get_value<T>(&mut self, num_bits: usize) -> Option<T> {
    assert!(num_bits <= 32);
    assert!(num_bits <= size_of::<T>() * 8);

    if self.byte_offset * 8 + self.bit_offset + num_bits > self.total_bytes * 8 {
      return None;
    }

    let mut v = trailing_bits(self.buffered_values, self.bit_offset + num_bits) >> self.bit_offset;
    self.bit_offset += num_bits;

    if self.bit_offset >= 64 {
      self.byte_offset += 8;
      self.bit_offset -= 64;

      let bytes_to_read = cmp::min(self.total_bytes - self.byte_offset, 8);
      self.buffered_values = read_num_bytes!(
        u64, bytes_to_read, self.buffer[self.byte_offset..], to_le);

      v |= trailing_bits(self.buffered_values, self.bit_offset) << (num_bits - self.bit_offset);
    }

    let result: T = unsafe {
      transmute_copy::<u64, T>(&v)
    };
    Some(result)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_bit_reader() {
    let mut buffer = vec![255, 0];
    let mut bit_reader = BitReader::new(buffer.as_mut_slice());
    let v1 = bit_reader.get_value::<i32>(1);
    assert!(v1.is_some());
    assert_eq!(v1.unwrap(), 1);
    let v2 = bit_reader.get_value::<i32>(2);
    assert!(v2.is_some());
    assert_eq!(v2.unwrap(), 3);
    let v3 = bit_reader.get_value::<i32>(3);
    assert!(v3.is_some());
    assert_eq!(v3.unwrap(), 7);
    let v4 = bit_reader.get_value::<i32>(4);
    assert!(v4.is_some());
    assert_eq!(v4.unwrap(), 3);
  }
}
