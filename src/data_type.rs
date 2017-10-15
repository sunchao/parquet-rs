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

use basic::Type;
use rand::{Rng, Rand};
use util::memory::Buffer;

// ----------------------------------------------------------------------
// Types connect Parquet physical types with Rust-specific types

// TODO: alignment?
// TODO: we could also use [u32; 3], however it seems there is no easy way
//   to convert [u32] to [u32; 3] in decoding.
#[derive(Clone, Debug)]
pub struct Int96 {
  value: Option<Vec<u32>>,
}

impl Int96 {
  pub fn new() -> Self {
    Int96 { value: None }
  }

  pub fn data(&self) -> &[u32] {
    assert!(self.value.is_some());
    &self.value.as_ref().unwrap()
  }

  pub fn set_data(&mut self, v: Vec<u32>) {
    assert_eq!(v.len(), 3);
    self.value = Some(v);
  }
}

impl Default for Int96 {
  fn default() -> Self { Int96 { value: None } }
}

impl PartialEq for Int96 {
  fn eq(&self, other: &Int96) -> bool {
    self.data() == other.data()
  }
}

impl From<Vec<u32>> for Int96 {
  fn from(buf: Vec<u32>) -> Int96 {
    assert_eq!(buf.len(), 3);
    Self { value: Some(buf) }
  }
}

impl Rand for Int96 {
  fn rand<R: Rng>(rng: &mut R) -> Self {
    let mut result = Int96::new();
    let mut value = vec!();
    for _ in 0..3 {
      value.push(rng.gen::<u32>());
    }
    result.set_data(value);
    result
  }
}


#[derive(Clone, Debug)]
pub struct ByteArray {
  data: Buffer
}

impl ByteArray {
  pub fn new() -> Self {
    ByteArray { data: Buffer::new() }
  }

  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn buffer(&self) -> Buffer {
    self.data.clone()
  }

  pub fn data(&self) -> &[u8] {
    self.data.data()
  }

  pub fn set_data(&mut self, data: Buffer) {
    self.data = data;
  }
}

impl<'a> From<&'a mut Vec<u8>> for ByteArray {
  fn from(v: &'a mut Vec<u8>) -> Self {
    Self {
      data: Buffer::from(v)
    }
  }
}

impl Default for ByteArray {
  fn default() -> Self { ByteArray::new() }
}

impl PartialEq for ByteArray {
  fn eq(&self, other: &ByteArray) -> bool {
    self.data() == other.data()
  }
}


// ----------------------------------------------------------------------
// AsBytes converts an instance of data type to a slice of u8

pub trait AsBytes {
  fn as_bytes(&self) -> &[u8];
}

macro_rules! gen_as_bytes {
  ($source_ty:ident) => {
    impl AsBytes for $source_ty {
      fn as_bytes(&self) -> &[u8] {
        unsafe {
          ::std::slice::from_raw_parts(
            self as *const $source_ty as *const u8, ::std::mem::size_of::<$source_ty>())
        }
      }
    }
  };
}

gen_as_bytes!(bool);
gen_as_bytes!(u8);
gen_as_bytes!(i32);
gen_as_bytes!(u32);
gen_as_bytes!(i64);
gen_as_bytes!(f32);
gen_as_bytes!(f64);

impl AsBytes for Int96 {
  fn as_bytes(&self) -> &[u8] {
    unsafe {
      ::std::slice::from_raw_parts(self.data() as *const [u32] as *const u8, 12)
    }
  }
}

impl AsBytes for ByteArray {
  fn as_bytes(&self) -> &[u8] {
    self.data()
  }
}

impl AsBytes for Vec<u8> {
  fn as_bytes(&self) -> &[u8] {
    self.as_slice()
  }
}

impl<'a> AsBytes for &'a str {
  fn as_bytes(&self) -> &[u8] {
    (self as &str).as_bytes()
  }
}

impl AsBytes for str {
  fn as_bytes(&self) -> &[u8] {
    (self as &str).as_bytes()
  }
}


// ----------------------------------------------------------------------
// DataType trait, which contains the Parquet physical type info as well as
// the Rust primitive type presentation.

pub trait DataType {
  type T: ::std::cmp::PartialEq + ::std::fmt::Debug + ::std::default::Default
    + ::std::clone::Clone + AsBytes;
  fn get_physical_type() -> Type;
  fn get_type_size() -> usize;
}

macro_rules! make_type {
  ($name:ident, $physical_ty:path, $native_ty:ty, $size:expr) => {
    pub struct $name {
    }

    impl DataType for $name {
      type T = $native_ty;

      fn get_physical_type() -> Type {
        $physical_ty
      }

      fn get_type_size() -> usize {
        $size
      }
    }
  };
}

/// Generate struct definitions for all physical types

make_type!(BoolType, Type::BOOLEAN, bool, 1);
make_type!(Int32Type, Type::INT32, i32, 4);
make_type!(Int64Type, Type::INT64, i64, 8);
make_type!(Int96Type, Type::INT96, Int96, mem::size_of::<Int96>());
make_type!(FloatType, Type::FLOAT, f32, 4);
make_type!(DoubleType, Type::DOUBLE, f64, 8);
make_type!(ByteArrayType, Type::BYTE_ARRAY, ByteArray, mem::size_of::<ByteArray>());
make_type!(FixedLenByteArrayType, Type::FIXED_LEN_BYTE_ARRAY,
           ByteArray, mem::size_of::<ByteArray>());


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_as_bytes() {
    assert_eq!(false.as_bytes(), &[0]);
    assert_eq!(true.as_bytes(), &[1]);
    assert_eq!((7 as i32).as_bytes(), &[7, 0, 0, 0]);
    assert_eq!((555 as i32).as_bytes(), &[43, 2, 0, 0]);
    assert_eq!((555 as u32).as_bytes(), &[43, 2, 0, 0]);
    assert_eq!(i32::max_value().as_bytes(), &[255, 255, 255, 127]);
    assert_eq!(i32::min_value().as_bytes(), &[0, 0, 0, 128]);
    assert_eq!((7 as i64).as_bytes(), &[7, 0, 0, 0, 0, 0, 0, 0]);
    assert_eq!((555 as i64).as_bytes(), &[43, 2, 0, 0, 0, 0, 0, 0]);
    assert_eq!((i64::max_value()).as_bytes(), &[255, 255, 255, 255, 255, 255, 255, 127]);
    assert_eq!((i64::min_value()).as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 128]);
    assert_eq!((3.14 as f32).as_bytes(), &[195, 245, 72, 64]);
    assert_eq!((3.14 as f64).as_bytes(), &[31, 133, 235, 81, 184, 30, 9, 64]);
    assert_eq!("hello".as_bytes(), &[b'h', b'e', b'l', b'l', b'o']);
    assert_eq!(Vec::from("hello".as_bytes()).as_bytes(), &[b'h', b'e', b'l', b'l', b'o']);

    // Test Int96
    let i96 = Int96::from(vec![1, 2, 3]);
    assert_eq!(i96.as_bytes(), &[1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]);

    // Test ByteArray
    let mut v = vec![1, 2, 3];
    let ba = ByteArray::from(&mut v);
    assert_eq!(ba.as_bytes(), &[1, 2, 3]);
  }
}
