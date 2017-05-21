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

use data_type::AsBytes;

/// Compute hash value for `data`, with a seed value `seed`.
/// The data type `T` must implement the `AsBytes` trait.
/// TODO: implement more efficient hash, such as Crc32, using SSE4 instructions.
pub fn hash<T: AsBytes>(data: &T, seed: u64) -> u64 {
  murmur_hash2_64a(data, seed)
}

static MURMUR_PRIME: u64 = 0xc6a4a7935bd1e995;
static MURMUR_R: i32 = 47;

/// Rust implementation of MurmurHash2, 64-bit version for 64-bit platforms
fn murmur_hash2_64a<T: AsBytes>(data: &T, seed: u64) -> u64 {
  let data_bytes = data.as_bytes();
  let len = data_bytes.len();
  let len_64 = (len / 8) * 8;
  let data_bytes_64 = unsafe {
    ::std::slice::from_raw_parts(&data_bytes[0..len_64] as *const [u8] as *const u64, len / 8)
  };

  let mut h = seed ^ (MURMUR_PRIME.wrapping_mul(data_bytes.len() as u64));
  for v in data_bytes_64 {
    let mut k = *v;
    k = k.wrapping_mul(MURMUR_PRIME);
    k ^= k >> MURMUR_R;
    k = k.wrapping_mul(MURMUR_PRIME);
    h ^= k;
    h = h.wrapping_mul(MURMUR_PRIME);
  }

  let data2 = &data_bytes[len_64..];

  // TODO: no pattern matching w/ fallthrough in Rust makes this very awkward
  match len & 7 {
    1 => {
      h ^= data2[0] as u64;
      h = h.wrapping_mul(MURMUR_PRIME)
    },
    2 => {
      h ^= (data2[1] as u64) << 8;
      h ^= data2[0] as u64;
      h = h.wrapping_mul(MURMUR_PRIME)
    },
    3 => {
      h ^= (data2[2] as u64) << 16;
      h ^= (data2[1] as u64) << 8;
      h ^= data2[0] as u64;
      h = h.wrapping_mul(MURMUR_PRIME)
    },
    4 => {
      h ^= (data2[3] as u64) << 24;
      h ^= (data2[2] as u64) << 16;
      h ^= (data2[1] as u64) << 8;
      h ^= data2[0] as u64;
      h = h.wrapping_mul(MURMUR_PRIME)
    },
    5 => {
      h ^= (data2[4] as u64) << 32;
      h ^= (data2[3] as u64) << 24;
      h ^= (data2[2] as u64) << 16;
      h ^= (data2[1] as u64) << 8;
      h ^= data2[0] as u64;
      h = h.wrapping_mul(MURMUR_PRIME)
    },
    6 => {
      h ^= (data2[5] as u64) << 40;
      h ^= (data2[4] as u64) << 32;
      h ^= (data2[3] as u64) << 24;
      h ^= (data2[2] as u64) << 16;
      h ^= (data2[1] as u64) << 8;
      h ^= data2[0] as u64;
      h = h.wrapping_mul(MURMUR_PRIME)
    },
    7 => {
      h ^= (data2[6] as u64) << 48;
      h ^= (data2[5] as u64) << 40;
      h ^= (data2[4] as u64) << 32;
      h ^= (data2[3] as u64) << 24;
      h ^= (data2[2] as u64) << 16;
      h ^= (data2[1] as u64) << 8;
      h ^= data2[0] as u64;
      h = h.wrapping_mul(MURMUR_PRIME)
    },
    _ => panic!("Impossible!")
  }

  h ^= h >> MURMUR_R;
  h = h.wrapping_mul(MURMUR_PRIME);
  h ^= h >> MURMUR_R;
  h
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_murmur2_64a() {
    let result = murmur_hash2_64a(&"hello", 123);
    assert_eq!(result, 2597646618390559622);

    let result = murmur_hash2_64a(&"helloworld", 123);
    assert_eq!(result, 4934371746140206573);

    let result = murmur_hash2_64a(&"helloworldparquet", 123);
    assert_eq!(result, 2392198230801491746);
  }

  #[test]
  fn test_hash() {
    let result = hash(&"hello", 123);
    assert_eq!(result, 2597646618390559622);

    let result = hash(&"helloworld", 123);
    assert_eq!(result, 4934371746140206573);

    let result = hash(&"helloworldparquet", 123);
    assert_eq!(result, 2392198230801491746);
  }
}
