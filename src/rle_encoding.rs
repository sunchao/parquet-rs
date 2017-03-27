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

use std::cmp;
use std::mem::{size_of, transmute_copy};

use errors::Result;
use util::bit_util::{self, BitReader};


// ----------------------------------------------------------------------
// RLE/Bit-Packing Hybrid Decoders

pub struct RleDecoder<'a> {
  /// Number of bits used to encode the value
  bit_width: usize,

  /// Bit reader loaded with input buffer.
  bit_reader: Option<BitReader<'a>>,

  /// The remaining number of values in RLE for this run
  rle_left: u32,

  /// The remaining number of values in Bit-Packing for this run
  bit_packing_left: u32,

  /// The current value for the case of RLE mode
  current_value: Option<u64>,
}

impl<'a> RleDecoder<'a> {
  pub fn new(bit_width: usize) -> Self {
    RleDecoder { bit_width: bit_width, rle_left: 0, bit_packing_left: 0,
                 bit_reader: None, current_value: None }
  }

  pub fn set_data(&mut self, data: &'a [u8]) {
    if let Some(ref mut bit_reader) = self.bit_reader {
      bit_reader.reset(data);
    } else {
      self.bit_reader = Some(BitReader::new(data));
    }

    let _ = self.reload();
  }

  pub fn decode<T: Default>(&mut self, buffer: &mut [T], max_values: usize) -> Result<usize> {
    assert!(buffer.len() >= max_values);
    assert!(self.bit_reader.is_some());
    assert!(size_of::<T>() <= 8);

    let mut values_read = 0;
    while values_read < max_values {
      if self.rle_left > 0 {
        assert!(self.current_value.is_some());
        let num_values = cmp::min(max_values - values_read, self.rle_left as usize);
        for i in 0..num_values {
          let repeated_value = unsafe {
            transmute_copy::<u64, T>(self.current_value.as_mut().unwrap())
          };
          buffer[values_read + i] = repeated_value;
        }
        self.rle_left -= num_values as u32;
        values_read += num_values;
      } else if self.bit_packing_left > 0 {
        assert!(self.bit_reader.is_some());
        let num_values = cmp::min(max_values - values_read, self.bit_packing_left as usize);
        if let Some(ref mut bit_reader) = self.bit_reader {
          for i in 0..num_values {
            bit_reader.get_value(self.bit_width).map(|v| {
              buffer[values_read + i] = v;
            })?;
          }
          self.bit_packing_left -= num_values as u32;
          values_read += num_values;
        }
      } else {
        if !self.reload() {
          break;
        }
      }
    }

    Ok(values_read)
  }

  pub fn decode_with_dict<T>(&mut self, dict: &[T], buffer: &mut [T],
                             max_values: usize) -> Result<usize> where T: Default + Clone {
    assert!(buffer.len() >= max_values);
    assert!(self.bit_width > 0);

    let mut values_read = 0;
    while values_read < max_values {
      if self.rle_left > 0 {
        assert!(self.current_value.is_some());
        let num_values = cmp::min(max_values - values_read, self.rle_left as usize);
        let dict_idx = self.current_value.unwrap() as usize;
        for i in 0..num_values {
          buffer[values_read + i] = dict[dict_idx].clone();
        }
        self.rle_left -= num_values as u32;
        values_read += num_values;
      } else if self.bit_packing_left > 0 {
        assert!(self.bit_reader.is_some());
        let num_values = cmp::min(max_values - values_read, self.bit_packing_left as usize);
        if let Some(ref mut bit_reader) = self.bit_reader {
          for i in 0..num_values {
            bit_reader.get_value::<i32>(self.bit_width).map(|v| {
              buffer[values_read + i] = dict[v as usize].clone();
            })?;
          }
          self.bit_packing_left -= num_values as u32;
          values_read += num_values;
        }
      } else {
        if !self.reload() {
          break;
        }
      }
    }

    Ok(values_read)
  }


  fn reload(&mut self) -> bool {
    assert!(self.bit_reader.is_some());
    if let Some(ref mut bit_reader) = self.bit_reader {
      if let Ok(indicator_value) = bit_reader.get_vlq_int() {
        if indicator_value & 1 == 1 {
          self.bit_packing_left = ((indicator_value >> 1) * 8) as u32;
        } else {
          self.rle_left = (indicator_value >> 1) as u32;
          let value_width = bit_util::ceil(self.bit_width as i64, 8);
          self.current_value = bit_reader.get_aligned::<u64>(value_width as usize).ok();
          assert!(self.current_value.is_some());
        }
        return true;
      }
    }
    return false;
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_rle_decode_int32() {
    // test data: 0-7 with bit width 3
    // 00000011 10001000 11000110 11111010
    let data = vec!(0x03, 0x88, 0xC6, 0xFA);
    let mut decoder: RleDecoder = RleDecoder::new(3);
    decoder.set_data(&data);
    let mut buffer = vec!(0; 8);
    let expected = vec!(0, 1, 2, 3, 4, 5, 6, 7);
    let result = decoder.decode::<i32>(&mut buffer, 8);
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

    let mut decoder: RleDecoder = RleDecoder::new(1);
    decoder.set_data(&data1);
    let mut buffer = vec!(false; 100);
    let mut expected = vec!();
    for i in 0..100 {
      if i < 50 {
        expected.push(true);
      } else {
        expected.push(false);
      }
    }
    let result = decoder.decode::<bool>(&mut buffer, 100);
    assert!(result.is_ok());
    assert_eq!(buffer, expected);

    decoder.set_data(&data2);
    let mut buffer = vec!(false; 100);
    let mut expected = vec!();
    for i in 0..100 {
      if i % 2 == 0 {
        expected.push(false);
      } else {
        expected.push(true);
      }
    }
    let result = decoder.decode::<bool>(&mut buffer, 100);
    assert!(result.is_ok());
    assert_eq!(buffer, expected);
  }

  #[test]
  fn test_rle_decode_with_dict_int32() {
    // test RLE encoding: 3 0s followed by 4 1s followed by 5 2s
    // 00000110 00000000 00001000 00000001 00001010 00000010
    let dict = vec!(10, 20, 30);
    let data = vec!(0x06, 0x00, 0x08, 0x01, 0x0A, 0x02);
    let mut decoder: RleDecoder = RleDecoder::new(3);
    decoder.set_data(&data);
    let mut buffer = vec!(0; 12);
    let expected = vec!(10, 10, 10, 20, 20, 20, 20, 30, 30, 30, 30, 30);
    let result = decoder.decode_with_dict::<i32>(&dict, &mut buffer, 12);
    assert!(result.is_ok());
    assert_eq!(buffer, expected);

    // test bit-pack encoding: 345345345455 (2 groups: 8 and 4)
    // 011 100 101 011 100 101 011 100 101 100 101 101
    // 00000011 01100011 11000111 10001110 00000011 01100101 00001011
    let dict = vec!("aaa", "bbb", "ccc", "ddd", "eee", "fff");
    let data = vec!(0x03, 0x63, 0xC7, 0x8E, 0x03, 0x65, 0x0B);
    let mut decoder: RleDecoder = RleDecoder::new(3);
    decoder.set_data(&data);
    let mut buffer = vec!(""; 12);
    let expected = vec!("ddd", "eee", "fff", "ddd", "eee", "fff",
                        "ddd", "eee", "fff", "eee", "fff", "fff");
    let result = decoder.decode_with_dict::<&str>(dict.as_slice(), buffer.as_mut_slice(), 12);
    assert!(result.is_ok());
    assert_eq!(buffer, expected);

  }
}
