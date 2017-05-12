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
use util::memory::BytePtr;

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
  data: Option<BytePtr>,
}

impl ByteArray {
  pub fn new() -> Self {
    ByteArray { data: None }
  }

  pub fn len(&self) -> usize {
    assert!(self.data.is_some());
    self.data.as_ref().unwrap().len()
  }

  pub fn data(&self) -> &[u8] {
    assert!(self.data.is_some());
    self.data.as_ref().unwrap().as_ref()
  }

  pub fn set_data(&mut self, data: BytePtr) {
    self.data = Some(data);
  }
}

impl Default for ByteArray {
  fn default() -> Self { ByteArray { data: None } }
}


impl PartialEq for ByteArray {
  fn eq(&self, other: &ByteArray) -> bool {
    self.data() == other.data()
  }
}

impl Rand for ByteArray {
  fn rand<R: Rng>(rng: &mut R) -> Self {
    let mut result = ByteArray::new();
    let mut value = vec!();
    let len = rng.gen_range::<usize>(0, 128);
    for _ in 0..len {
      value.push(rng.gen_range(0, 255) & 0xFF);
    }
    result.set_data(BytePtr::new(value));
    result
  }
}


pub trait DataType {
  type T: ::std::cmp::PartialEq + ::std::fmt::Debug + ::std::default::Default
    + ::std::clone::Clone + Rand;
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
