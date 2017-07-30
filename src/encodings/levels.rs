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

use basic::Encoding;
use data_type::AsBytes;
use errors::Result;
use util::bit_util::log2;
use util::memory::ByteBufferPtr;
use super::rle_encoding::{RleEncoder, RleDecoder};


/// A encoder for definition/repetition levels. This is a thin
/// wrapper on `RleEncoder`. Currently only support RLE encoding.

// TODO: track memory usage.
pub struct LevelEncoder {
  encoding: Encoding,
  bit_width: usize,
  rle_encoder: RleEncoder
}

impl LevelEncoder {
  pub fn new(encoding: Encoding, max_level: i16) -> Self {
    assert!(encoding == Encoding::RLE, "Currently only support RLE encoding");
    let bit_width = log2(max_level as u64 + 1) as usize;
    let max_buffer_size = RleEncoder::min_buffer_size(bit_width);
    let buffer = vec![0; max_buffer_size + mem::size_of::<i32>()];
    Self {
      encoding: encoding,
      bit_width: bit_width,
      rle_encoder: RleEncoder::new_from_buf(bit_width, buffer, mem::size_of::<i32>())
    }
  }

  #[inline]
  pub fn put(&mut self, buffer: &[i16]) -> Result<i16> {
    let mut num_encoded = 0;
    for v in buffer {
      if !self.rle_encoder.put(*v as u64)? { break; }
      num_encoded += 1;
    }
    Ok(num_encoded)
  }

  #[inline]
  pub fn consume(&mut self) -> Result<Vec<u8>> {
    let len = (self.rle_encoder.len() as i32).to_le();
    let len_bytes = len.as_bytes();
    let mut encoded_data = self.rle_encoder.consume()?;
    encoded_data[0..len_bytes.len()].copy_from_slice(len_bytes);
    Ok(encoded_data)
  }
}

/// A encoder for definition/repetition levels. This is a thin
/// wrapper on `RleDecoder`. Currently it only support RLE encoding.
pub struct LevelDecoder {
  encoding: Encoding,
  bit_width: usize,
  rle_decoder: RleDecoder
}

impl LevelDecoder {
  pub fn new(encoding: Encoding, max_level: i16) -> Self {
    assert!(encoding == Encoding::RLE, "Currently only support RLE encoding");
    let bit_width = log2(max_level as u64) as usize;
    Self {
      encoding: encoding,
      bit_width: bit_width,
      rle_decoder: RleDecoder::new(bit_width),
    }
  }

  #[inline]
  pub fn set_data(&mut self, data: ByteBufferPtr) -> usize {
    let i32_size = mem::size_of::<i32>();
    let data_size = read_num_bytes!(i32, i32_size, data.as_ref()) as usize;
    self.rle_decoder.set_data(data.range(i32_size, data_size));
    i32_size + data_size
  }

  #[inline]
  pub fn get(&mut self, buffer: &mut [i16]) -> Result<usize> {
    self.rle_decoder.get_batch::<i16>(buffer)
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_roundtrip() {
    let max_level = 10;
    let mut encoder = LevelEncoder::new(Encoding::RLE, max_level);
    let data = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
    encoder.put(&data).expect("put() should be OK");

    let buffer = encoder.consume().expect("consume() should be OK");
    let buffer_len = buffer.len();
    let mut decoder = LevelDecoder::new(Encoding::RLE, max_level);
    assert_eq!(decoder.set_data(ByteBufferPtr::new(buffer)), buffer_len);
    let mut result: Vec<i16> = vec![0; 10];
    let num_decoded = decoder.get(&mut result).expect("get() should be OK");
    assert_eq!(num_decoded, 10);
    assert_eq!(result, data);
  }
}
