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

use parquet::basic::*;
use parquet::data_type::*;
use parquet::encodings::encoding::*;
use parquet::encodings::decoding::*;
use parquet::schema::types::{Type as SchemaType, ColumnDescriptor, ColumnPath};
use parquet::util::memory::{ByteBufferPtr, MemTracker};

macro_rules! plain_int {
  ($fname:ident, $num_values:expr, $batch_size:expr, $low:expr, $high:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let mut encoder = PlainEncoder::<Int32Type>::new(
        Rc::new(col_desc(0, Type::INT32)), mem_tracker, vec!());

      let mut values = Vec::with_capacity($num_values);
      let mut rng = thread_rng();
      for _ in 0..$num_values {
        values.push(rng.gen_range::<i32>($low, $high));
      }
      encoder.put(&values[..]).expect("put() should be OK");
      let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");

      let decoder = PlainDecoder::<Int32Type>::new(0);
      bench_int_decoding(bench, $num_values, $batch_size, buffer, Box::new(decoder));
    }
  }
}

macro_rules! dict_int {
  ($fname:ident, $num_values:expr, $batch_size:expr, $low:expr, $high:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mem_tracker = Rc::new(MemTracker::new());
      let mut encoder = DictEncoder::<Int32Type>::new(
        Rc::new(col_desc(0, Type::INT32)), mem_tracker);

      let mut values = Vec::with_capacity($num_values);
      let mut rng = thread_rng();
      for _ in 0..$num_values {
        values.push(rng.gen_range::<i32>($low, $high));
      }
      encoder.put(&values[..]).expect("put() should be OK");
      let mut dict_decoder = PlainDecoder::<Int32Type>::new(0);
      dict_decoder.set_data(
        encoder.write_dict().expect("write_dict() should be OK"),
        encoder.num_entries()).expect("set_data() should be OK");

      let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");
      let mut decoder = DictDecoder::<Int32Type>::new();
      decoder.set_dict(Box::new(dict_decoder)).expect("set_dict() should be OK");

      bench_int_decoding(bench, $num_values, $batch_size, buffer, Box::new(decoder));
    }
  }
}

macro_rules! delta_bit_pack_int {
  ($fname:ident, $num_values:expr, $batch_size:expr, $low:expr, $high:expr) => {
    #[bench]
    fn $fname(bench: &mut Bencher) {
      let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();

      let mut values = Vec::with_capacity($num_values);
      let mut rng = thread_rng();
      for _ in 0..$num_values {
        values.push(rng.gen_range::<i32>($low, $high));
      }
      encoder.put(&values[..]).expect("put() should be OK");
      let buffer = encoder.flush_buffer().expect("flush_buffer() should be OK");

      let decoder = DeltaBitPackDecoder::<Int32Type>::new();
      bench_int_decoding(bench, $num_values, $batch_size, buffer, Box::new(decoder));
    }
  }
}

fn bench_int_decoding(
  bench: &mut Bencher,
  num_values: usize,
  batch_size: usize,
  buffer: ByteBufferPtr,
  mut decoder: Box<Decoder<Int32Type>>
) {
  bench.iter(|| {
    decoder.set_data(buffer.clone(), num_values).expect("set_data() should be OK");
    let mut values = vec![0; batch_size];
    loop {
      if decoder.get(&mut values[..]).expect("get() should be OK") < batch_size {
        break
      }
    }
  })
}

fn col_desc(type_length: i32, primitive_ty: Type) -> ColumnDescriptor {
  let ty = SchemaType::primitive_type_builder("col", primitive_ty)
    .with_length(type_length)
    .build()
    .unwrap();
  ColumnDescriptor::new(Rc::new(ty), None, 0, 0, ColumnPath::new(vec!()))
}


plain_int!(plain_i32_1k_32, 1024, 32, 0, 1000);
plain_int!(plain_i32_1k_64, 1024, 64, 0, 1000);
plain_int!(plain_i32_1k_128, 1024, 128, 0, 1000);
plain_int!(plain_i32_1m_32, 1024, 32, 0, 1000);
plain_int!(plain_i32_1m_64, 1024, 64, 0, 1000);
plain_int!(plain_i32_1m_128, 1024, 128, 0, 1000);

dict_int!(dict_i32_1k_32, 1024, 32, 0, 1000);
dict_int!(dict_i32_1k_64, 1024, 64, 0, 1000);
dict_int!(dict_i32_1k_128, 1024, 128, 0, 1000);
dict_int!(dict_i32_1m_32, 1024 * 1024, 32, 0, 1000);
dict_int!(dict_i32_1m_64, 1024 * 1024, 64, 0, 1000);
dict_int!(dict_i32_1m_128, 1024 * 1024, 128, 0, 1000);

delta_bit_pack_int!(delta_bit_pack_i32_1k_32, 1024, 32, 0, 1000);
delta_bit_pack_int!(delta_bit_pack_i32_1k_64, 1024, 64, 0, 1000);
delta_bit_pack_int!(delta_bit_pack_i32_1k_128, 1024, 128, 0, 1000);
delta_bit_pack_int!(delta_bit_pack_i32_1m_32, 1024 * 1024, 32, 0, 1000);
delta_bit_pack_int!(delta_bit_pack_i32_1m_64, 1024 * 1024, 64, 0, 1000);
delta_bit_pack_int!(delta_bit_pack_i32_1m_128, 1024 * 1024, 128, 0, 1000);
