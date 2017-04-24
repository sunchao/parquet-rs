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

use rand::{thread_rng, Rng, Rand};
use rand::distributions::range::SampleRange;

pub fn random_bytes(n: usize) -> Vec<u8> {
  let mut result = vec!();
  let mut rng = thread_rng();
  for _ in 0..n {
    result.push(rng.gen_range(0, 255) & 0xFF);
  }
  result
}

pub fn random_bools(n: usize) -> Vec<bool> {
  let mut result = vec!();
  let mut rng = thread_rng();
  for _ in 0..n {
    result.push(rng.gen::<bool>());
  }
  result
}

pub fn random_numbers<T: Rand>(n: usize) -> Vec<T> {
  let mut result = vec!();
  let mut rng = thread_rng();
  for _ in 0..n {
    result.push(rng.gen::<T>());
  }
  result
}

pub fn random_numbers_range<T>(n: usize, low: T, high: T) -> Vec<T>
    where T: PartialOrd + SampleRange + Copy {
  let mut result = vec!();
  let mut rng = thread_rng();
  for _ in 0..n {
    result.push(rng.gen_range(low, high));
  }
  result
}
