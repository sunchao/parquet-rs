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
pub struct LevelEncoder {
  encoding: Encoding,
  bit_width: u8,
  rle_encoder: RleEncoder
}

impl LevelEncoder {
  pub fn new(encoding: Encoding, max_level: i16, byte_buffer: Vec<u8>) -> Self {
    assert!(encoding == Encoding::RLE, "Currently only support RLE encoding");
    let bit_width = log2(max_level as u64 + 1) as u8;

    Self {
      encoding: encoding,
      bit_width: bit_width,
      rle_encoder: RleEncoder::new_from_buf(bit_width, byte_buffer, mem::size_of::<i32>())
    }
  }

  #[inline]
  pub fn put(&mut self, buffer: &[i16]) -> Result<i16> {
    let mut num_encoded = 0;
    for v in buffer {
      if !self.rle_encoder.put(*v as u64)? { break; }
      num_encoded += 1;
    }
    self.rle_encoder.flush()?;
    Ok(num_encoded)
  }

  #[inline]
  pub fn max_buffer_size(encoding: Encoding, max_level: i16, num_buffered_values: usize) -> usize {
    let bit_width = log2(max_level as u64 + 1) as u8;
    match encoding {
      Encoding::RLE => {
        RleEncoder::max_buffer_size(bit_width, num_buffered_values) +
          RleEncoder::min_buffer_size(bit_width)
      },
      _ => panic!("Unsupported encoding type {}", encoding)
    }
  }

  #[inline]
  pub fn consume(mut self) -> Result<Vec<u8>> {
    self.rle_encoder.flush()?;
    let len = (self.rle_encoder.len() as i32).to_le();
    let len_bytes = len.as_bytes();
    let mut encoded_data = self.rle_encoder.consume();
    encoded_data[0..len_bytes.len()].copy_from_slice(len_bytes);
    Ok(encoded_data)
  }
}

/// A encoder for definition/repetition levels. This is a thin
/// wrapper on `RleDecoder`. Currently it only support RLE encoding.
pub struct LevelDecoder {
  encoding: Encoding,
  bit_width: u8,
  rle_decoder: RleDecoder
}

impl LevelDecoder {
  pub fn new(encoding: Encoding, max_level: i16) -> Self {
    assert!(encoding == Encoding::RLE, "Currently only support RLE encoding");
    let bit_width = log2(max_level as u64 + 1) as u8;
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

  // Set byte array explicitly when start position ('start') and length ('len') are known in advance
  // Returns number of total bytes set for this decoder (len)
  #[inline]
  pub fn set_data_range(&mut self, data: &ByteBufferPtr, start: usize, len: usize) -> usize {
    self.rle_decoder.set_data(data.range(start, len));
    len
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
  fn test_roundtrip_one() {
    let max_level = 1;
    let levels = vec![0, 1, 1, 1, 1, 0, 0, 0, 0, 1];
    let max_buffer_size = LevelEncoder::max_buffer_size(Encoding::RLE, max_level, levels.len());
    let mut encoder = LevelEncoder::new(Encoding::RLE, max_level, vec![0; max_buffer_size]);
    encoder.put(&levels).expect("put() should be OK");
    let encoded_levels = encoder.consume().expect("consume() should be OK");

    let mut decoder = LevelDecoder::new(Encoding::RLE, max_level);
    decoder.set_data(ByteBufferPtr::new(encoded_levels));
    let mut buffer = vec![0; levels.len()];
    let num_decoded = decoder.get(&mut buffer).expect("get() should be OK");
    assert_eq!(num_decoded, levels.len());
    assert_eq!(buffer, levels);
  }

  #[test]
  fn test_roundtrip() {
    let max_level = 10;
    let buffer = vec![0; LevelEncoder::max_buffer_size(Encoding::RLE, max_level, 10)];
    let mut encoder = LevelEncoder::new(Encoding::RLE, max_level, buffer);
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

  #[test]
  fn test_decoder_set_data_range() {
    // buffer containing both repetition and definition levels
    let buffer = ByteBufferPtr::new(vec![5, 198, 2, 5, 42, 168, 10, 0, 2, 3, 36, 73]);

    let max_rep_level = 1;
    let mut decoder = LevelDecoder::new(Encoding::RLE, max_rep_level);
    assert_eq!(decoder.set_data_range(&buffer, 0, 3), 3);
    let mut result = vec![0; 10];
    let num_decoded = decoder.get(&mut result).expect("get() should be OK");
    assert_eq!(num_decoded, 10);
    assert_eq!(result, vec![0, 1, 1, 0, 0, 0, 1, 1, 0, 1]);

    let max_def_level = 2;
    let mut decoder = LevelDecoder::new(Encoding::RLE, max_def_level);
    assert_eq!(decoder.set_data_range(&buffer, 3, 5), 5);
    let mut result = vec![0; 10];
    let num_decoded = decoder.get(&mut result).expect("get() should be OK");
    assert_eq!(num_decoded, 10);
    assert_eq!(result, vec![2, 2, 2, 0, 0, 2, 2, 2, 2, 2]);
  }
}
