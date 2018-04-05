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

use std::cell;
use std::convert;
use std::io;
use std::result;

use snap;
use thrift;

quick_error! {
  #[derive(Debug, PartialEq)]
  pub enum ParquetError {
    General(message: String) {
      display("Parquet error: {}", message)
      description(message)
      from(e: io::Error) -> (format!("underlying IO error: {}", e))
      from(e: snap::Error) -> (format!("underlying snap error: {}", e))
      from(e: thrift::Error) -> (format!("underlying Thrift error: {}", e))
      from(e: cell::BorrowMutError) -> (format!("underlying borrow error: {}", e))
    }
    NYI(message: String) {
      display("NYI: {}", message)
      description(message)
    }
    EOF(message: String) {
      display("EOF: {}", message)
      description(message)
    }
  }
}

pub type Result<T> = result::Result<T, ParquetError>;

/// Conversion from `ParquetError` TO other types of `Error`s

impl convert::From<ParquetError> for io::Error {
  fn from(e: ParquetError) -> Self {
    io::Error::new(io::ErrorKind::Other, e)
  }
}

/// Convenient macros for different errors

macro_rules! general_err {
  ($fmt:expr) => (ParquetError::General($fmt.to_owned()));
  ($fmt:expr, $($args:expr),*) => (ParquetError::General(format!($fmt, $($args),*)));
  ($e:expr, $fmt:expr) => (ParquetError::General($fmt.to_owned(), $e));
  ($e:ident, $fmt:expr, $($args:tt),*) => (
    ParquetError::General(&format!($fmt, $($args),*), $e));
}

macro_rules! nyi_err {
  ($fmt:expr) => (ParquetError::NYI($fmt.to_owned()));
  ($fmt:expr, $($args:expr),*) => (ParquetError::NYI(format!($fmt, $($args),*)));
}

macro_rules! eof_err {
  ($fmt:expr) => (ParquetError::EOF($fmt.to_owned()));
  ($fmt:expr, $($args:expr),*) => (ParquetError::EOF(format!($fmt, $($args),*)));
}
