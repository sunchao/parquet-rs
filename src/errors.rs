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

use std::io;
use std::result;
use thrift;
use snap;

quick_error! {
  #[derive(Debug)]
  pub enum ParquetError {
    Parse(message: String) {
      display("Parse error: {}", message)
      description(message)
    }
    Io(message: String, err: io::Error) {
      from(err: io::Error) -> ("underlying IO error".to_owned(), err)
      from(err: snap::Error) -> ("underlying snap error".to_owned(), io::Error::from(err))
      display("IO error: {}, {}", message, err)
    }
    Decode(message: String) {
      display("Decoding error: {}", message)
    }
    Thrift(message: String, err: thrift::Error) {
      from(err: thrift::Error) -> ("underlying Thrift error".to_owned(), err)
      display("Thrift error: {}, {}", message, err)
    }
    Schema(message: String) {
      display("Schema error: {}", message)
    }
    Unsupported(message: String) {
      display("Unsupported error: {}", message)
    }
  }
}

pub type Result<T> = result::Result<T, ParquetError>;

/// Convenient macros for different errors
macro_rules! parse_err {
  ($fmt:expr) => (ParquetError::Parse($fmt.to_owned()));
  ($fmt:expr, $($args:tt),*) => (ParquetError::Parse(format!($fmt, $($args),*)));
}

macro_rules! schema_err {
  ($fmt:expr) => (ParquetError::Schema($fmt.to_owned()));
  ($fmt:expr, $($args:expr),*) => (ParquetError::Schema(format!($fmt, $($args),*)));
}

macro_rules! decode_err {
  ($fmt:expr) => (ParquetError::Decode($fmt.to_owned()));
  ($fmt:expr, $($args:expr),*) => (ParquetError::Decode(format!($fmt, $($args),*)));
}

macro_rules! unsupported_err {
  ($fmt:expr) => (ParquetError::Unsupported($fmt.to_owned()));
  ($fmt:expr, $($args:expr),*) => (ParquetError::Unsupported(format!($fmt, $($args),*)));
}

macro_rules! io_err {
  ($e:expr, $fmt:expr) => (ParquetError::Io($fmt.to_owned(), $e));
  ($e:ident, $fmt:expr, $($args:tt),*) => (
    ParquetError::Io(&format!($fmt, $($args),*), $e));
}

macro_rules! thrift_err {
  ($e:ident, $fmt:expr) => (ParquetError::Thrift($fmt.to_owned(), $e));
  ($e:ident, $fmt:expr, $($args:tt),*) => (
    ParquetError::Thrift(format!($fmt, $($args),*), $e));
}
