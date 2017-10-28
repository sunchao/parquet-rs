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
extern crate parquet;
use test::Bencher;

use std::rc::Rc;
use parquet::basic::*;
use parquet::data_type::*;
use parquet::encodings::encoding::*;
use parquet::schema::types::{Type as SchemaType, ColumnDescriptor, ColumnPath};
use parquet::util::memory::MemTracker;

const BATCH_SIZE: usize = 1024;

#[bench]
fn plain_encoding_bool(bench: &mut Bencher) {
  plain_encoding::<BoolType>(bench, vec![false; BATCH_SIZE], 1, Type::BOOLEAN);
}

#[bench]
fn plain_encoding_i32(bench: &mut Bencher) {
  plain_encoding::<Int32Type>(bench, vec![0; BATCH_SIZE], 4, Type::INT32);
}

#[bench]
fn plain_encoding_i64(bench: &mut Bencher) {
  plain_encoding::<Int64Type>(bench, vec![0; BATCH_SIZE], 8, Type::INT64);
}

fn plain_encoding<T: DataType>(
  bench: &mut Bencher, data: Vec<T::T>, type_length: i32, primitive_ty: Type
) {
  let mem_tracker = MemTracker::new_ptr(None).expect("");
  let mut encoder = PlainEncoder::<T>::new(
    Rc::new(col_desc(type_length, primitive_ty)), mem_tracker, vec!());
  bench.iter(|| {
    let _ = encoder.put(&data[..]);
  })
}

fn col_desc(type_length: i32, primitive_ty: Type) -> ColumnDescriptor {
  let ty = SchemaType::primitive_type_builder("col", primitive_ty)
    .with_length(type_length)
    .build()
    .unwrap();
  ColumnDescriptor::new(Rc::new(ty), None, 0, 0, ColumnPath::new(vec!()))
}
