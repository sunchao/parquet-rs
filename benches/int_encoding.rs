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

#![feature(test)]
extern crate test;
extern crate rand;
extern crate parquet;
use test::Bencher;

use std::rc::Rc;
use rand::{thread_rng, Rng};
use rand::distributions::range::SampleRange;

use parquet::basic::*;
use parquet::data_type::*;
use parquet::encodings::encoding::*;
use parquet::schema::types::{Type as SchemaType, ColumnDescriptor, ColumnPath};
use parquet::util::memory::MemTracker;

macro_rules! plain_int {
  ($fname:ident, $batch_size:expr, $low:expr, $high:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let encoder = PlainEncoder::<Int32Type>::new(
        Rc::new(col_desc(0, Type::INT32)), mem_tracker, vec!());
      bench_int_encoding(bench, $batch_size, $low, $high, Box::new(encoder));
    }
  }
}

macro_rules! dict_int {
  ($fname:ident, $batch_size:expr, $low:expr, $high:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let encoder = DictEncoder::<Int32Type>::new(
        Rc::new(col_desc(0, Type::INT32)), mem_tracker);
      bench_int_encoding(bench, $batch_size, $low, $high, Box::new(encoder));
    }
  }
}

macro_rules! delta_bit_pack_int {
  ($fname:ident, $batch_size:expr, $low:expr, $high:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let encoder = DeltaBitPackEncoder::<Int32Type>::new();
      bench_int_encoding(bench, $batch_size, $low, $high, Box::new(encoder));
    }
  }
}

fn bench_int_encoding<T: DataType>(
  bench: &mut Bencher,
  batch_size: usize,
  low: T::T,
  high: T::T,
  mut encoder: Box<Encoder<T>>
) where T::T: PartialOrd + SampleRange + Copy {
  let mut values = Vec::with_capacity(batch_size);
  let mut rng = thread_rng();
  for _ in 0..batch_size {
    values.push(rng.gen_range::<T::T>(low, high));
  }

  bench.iter(|| {
    encoder.put(&values[..]).expect("put() should be OK");
    encoder.flush_buffer().expect("flush_buffer() should be OK");
  })
}

fn col_desc(type_length: i32, primitive_ty: Type) -> ColumnDescriptor {
  let ty = SchemaType::primitive_type_builder("col", primitive_ty)
    .with_length(type_length)
    .build()
    .unwrap();
  ColumnDescriptor::new(Rc::new(ty), None, 0, 0, ColumnPath::new(vec!()))
}


plain_int!(plain_i32_1k_10, 1024, 0, 10);
plain_int!(plain_i32_1k_100, 1024, 0, 100);
plain_int!(plain_i32_1k_1000, 1024, 0, 1000);
plain_int!(plain_i32_1m_10, 1024 * 1024, 0, 10);
plain_int!(plain_i32_1m_100, 1024 * 1024, 0, 100);
plain_int!(plain_i32_1m_1000, 1024 * 1024, 0, 1000);

dict_int!(dict_i32_1k_10, 1024, 0, 10);
dict_int!(dict_i32_1k_100, 1024, 0, 100);
dict_int!(dict_i32_1k_1000, 1024, 0, 1000);
dict_int!(dict_i32_1m_10, 1024 * 1024, 0, 10);
dict_int!(dict_i32_1m_100, 1024 * 1024, 0, 100);
dict_int!(dict_i32_1m_1000, 1024 * 1024, 0, 1000);

delta_bit_pack_int!(delta_bit_pack_i32_1k_10, 1024, 0, 10);
delta_bit_pack_int!(delta_bit_pack_i32_1k_100, 1024, 0, 100);
delta_bit_pack_int!(delta_bit_pack_i32_1k_1000, 1024, 0, 1000);
delta_bit_pack_int!(delta_bit_pack_i32_1m_10, 1024 * 1024, 0, 10);
delta_bit_pack_int!(delta_bit_pack_i32_1m_100, 1024 * 1024, 0, 100);
delta_bit_pack_int!(delta_bit_pack_i32_1m_1000, 1024 * 1024, 0, 1000);
