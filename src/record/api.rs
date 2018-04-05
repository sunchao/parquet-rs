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

use std::fmt;

use basic::{LogicalType, Type as PhysicalType};
use data_type::{ByteArray, Int96};

/// Macro as a shortcut to generate 'not yet implemented' panic error.
macro_rules! nyi {
  ($physical_type:ident, $logical_type:ident, $value:ident) => ({
    unimplemented!(
      "Conversion for physical type {}, logical type {}, value {:?}",
      $physical_type,
      $logical_type,
      $value
    );
  });
}

/// Row API to represent nested Parquet record.
/// Use implementation methods to update how Parquet types are mapped to Row.
#[derive(Clone, Debug, PartialEq)]
pub enum Row {
  // Primitive types
  Null,
  Bool(bool),
  Byte(i8),
  Short(i16),
  Int(i32),
  Long(i64),
  Float(f32),
  Double(f64),
  Str(String),
  Bytes(ByteArray),
  Timestamp(u64), // Timestamp with milliseconds
  // Complex types
  Group(Vec<(String, Row)>), // Struct, child elements are tuples of field-value pairs
  List(Vec<Row>), // List of elements
  Map(Vec<(Row, Row)>) // List of key-value pairs
}

impl Row {
  /// Converts BOOLEAN into boolean value.
  pub fn convert_bool(
    _physical_type: PhysicalType,
    _logical_type: LogicalType,
    value: bool
  ) -> Self {
    Row::Bool(value)
  }

  // Converts INT32 into integer value.
  pub fn convert_int32(
    physical_type: PhysicalType,
    logical_type: LogicalType,
    value: i32
  ) -> Self {
    match logical_type {
      LogicalType::INT_8 => Row::Byte(value as i8),
      LogicalType::INT_16 => Row::Short(value as i16),
      LogicalType::INT_32 | LogicalType::NONE => Row::Int(value),
      _ => nyi!(physical_type, logical_type, value)
    }
  }

  /// Converts INT64 into long value.
  pub fn convert_int64(
    physical_type: PhysicalType,
    logical_type: LogicalType,
    value: i64
  ) -> Self {
    match logical_type {
      LogicalType::INT_64 | LogicalType::NONE => Row::Long(value),
      _ => nyi!(physical_type, logical_type, value)
    }
  }

  /// Converts nanosecond timestamps stored as INT96 into milliseconds
  pub fn convert_int96(
    _physical_type: PhysicalType,
    _logical_type: LogicalType,
    value: Int96
  ) -> Self {
    let julian_to_unix_epoch_days: u64 = 2_440_588;
    let milli_seconds_in_a_day: u64 = 86_400_000;
    let nano_seconds_in_a_day: u64 = milli_seconds_in_a_day * 1_000_000;

    let days_since_epoch = value.data()[2] as u64 - julian_to_unix_epoch_days;
    let nanoseconds: u64 = ((value.data()[1] as u64) << 32) + value.data()[0] as u64;
    let nanos = days_since_epoch * nano_seconds_in_a_day + nanoseconds;
    let millis = nanos / 1_000_000;

    Row::Timestamp(millis)
  }

  /// Converts FLOAT into float value.
  pub fn convert_float(
    _physical_type: PhysicalType,
    _logical_type: LogicalType,
    value: f32
  ) -> Self {
    Row::Float(value)
  }

  /// Converts DOUBLE into double value.
  pub fn convert_double(
    _physical_type: PhysicalType,
    _logical_type: LogicalType,
    value: f64
  ) -> Self {
    Row::Double(value)
  }

  /// Converts BYTE_ARRAY into either UTF8 string or array of bytes.
  pub fn convert_byte_array(
    physical_type: PhysicalType,
    logical_type: LogicalType,
    value: ByteArray
  ) -> Self {
    match physical_type {
      PhysicalType::BYTE_ARRAY => {
        match logical_type {
          LogicalType::UTF8 | LogicalType::ENUM | LogicalType::JSON => {
            let value = unsafe { String::from_utf8_unchecked(value.data().to_vec()) };
            Row::Str(value)
          },
          LogicalType::BSON | LogicalType::NONE => Row::Bytes(value),
          _ => nyi!(physical_type, logical_type, value)
        }
      },
      _ => nyi!(physical_type, logical_type, value)
    }
  }
}

impl fmt::Display for Row {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Row::Null => write!(f, "null"),
      Row::Bool(value) => write!(f, "{}", value),
      Row::Byte(value) => write!(f, "{}", value),
      Row::Short(value) => write!(f, "{}", value),
      Row::Int(value) => write!(f, "{}", value),
      Row::Long(value) => write!(f, "{}", value),
      Row::Float(value) => write!(f, "{:?}", value),
      Row::Double(value) => write!(f, "{:?}", value),
      Row::Str(ref value) => write!(f, "\"{}\"", value),
      Row::Bytes(ref value) => write!(f, "{:?}", value.data()),
      Row::Timestamp(value) => write!(f, "{}", value),
      Row::Group(ref fields) => {
        write!(f, "{{")?;
        for (i, &(ref key, ref value)) in fields.iter().enumerate() {
          key.fmt(f)?;
          write!(f, ": ")?;
          value.fmt(f)?;
          if i < fields.len() - 1 {
            write!(f, ", ")?;
          }
        }
        write!(f, "}}")
      },
      Row::List(ref fields) => {
        write!(f, "[")?;
        for (i, field) in fields.iter().enumerate() {
          field.fmt(f)?;
          if i < fields.len() - 1 {
            write!(f, ", ")?;
          }
        }
        write!(f, "]")
      },
      Row::Map(ref pairs) => {
        write!(f, "{{")?;
        for (i, &(ref key, ref value)) in pairs.iter().enumerate() {
          key.fmt(f)?;
          write!(f, " -> ")?;
          value.fmt(f)?;
          if i < pairs.len() - 1 {
            write!(f, ", ")?;
          }
        }
        write!(f, "}}")
      }
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_row_convert_bool() {
    // BOOLEAN value does not depend on logical type
    let row = Row::convert_bool(PhysicalType::BOOLEAN, LogicalType::NONE, true);
    assert_eq!(row, Row::Bool(true));

    let row = Row::convert_bool(PhysicalType::BOOLEAN, LogicalType::NONE, false);
    assert_eq!(row, Row::Bool(false));
  }

  #[test]
  fn test_row_convert_int32() {
    let row = Row::convert_int32(PhysicalType::INT32, LogicalType::INT_8, 111);
    assert_eq!(row, Row::Byte(111));

    let row = Row::convert_int32(PhysicalType::INT32, LogicalType::INT_16, 222);
    assert_eq!(row, Row::Short(222));

    let row = Row::convert_int32(PhysicalType::INT32, LogicalType::INT_32, 333);
    assert_eq!(row, Row::Int(333));

    let row = Row::convert_int32(PhysicalType::INT32, LogicalType::NONE, 444);
    assert_eq!(row, Row::Int(444));
  }

  #[test]
  fn test_row_convert_int64() {
    let row = Row::convert_int64(PhysicalType::INT64, LogicalType::INT_64, 1111);
    assert_eq!(row, Row::Long(1111));

    let row = Row::convert_int64(PhysicalType::INT64, LogicalType::NONE, 2222);
    assert_eq!(row, Row::Long(2222));
  }

  #[test]
  fn test_row_convert_int96() {
    // INT96 value does not depend on logical type
    let value = Int96::from(vec![0, 0, 2454923]);
    let row = Row::convert_int96(PhysicalType::INT96, LogicalType::NONE, value);
    assert_eq!(row, Row::Timestamp(1238544000000));

    let value = Int96::from(vec![4165425152, 13, 2454923]);
    let row = Row::convert_int96(PhysicalType::INT96, LogicalType::NONE, value);
    assert_eq!(row, Row::Timestamp(1238544060000));
  }

  #[test]
  fn test_row_convert_float() {
    // FLOAT value does not depend on logical type
    let row = Row::convert_float(PhysicalType::FLOAT, LogicalType::NONE, 2.31);
    assert_eq!(row, Row::Float(2.31));
  }

  #[test]
  fn test_row_convert_double() {
    // DOUBLE value does not depend on logical type
    let row = Row::convert_double(PhysicalType::FLOAT, LogicalType::NONE, 1.56);
    assert_eq!(row, Row::Double(1.56));
  }

  #[test]
  fn test_row_convert_byte_array() {
    // UTF8
    let value = ByteArray::from(vec![b'A', b'B', b'C', b'D']);
    let row = Row::convert_byte_array(
      PhysicalType::BYTE_ARRAY, LogicalType::UTF8, value);
    assert_eq!(row, Row::Str("ABCD".to_string()));

    // ENUM
    let value = ByteArray::from(vec![b'1', b'2', b'3']);
    let row = Row::convert_byte_array(
      PhysicalType::BYTE_ARRAY, LogicalType::ENUM, value);
    assert_eq!(row, Row::Str("123".to_string()));

    // JSON
    let value = ByteArray::from(vec![b'{', b'"', b'a', b'"', b':', b'1', b'}']);
    let row = Row::convert_byte_array(
      PhysicalType::BYTE_ARRAY, LogicalType::JSON, value);
    assert_eq!(row, Row::Str("{\"a\":1}".to_string()));

    // NONE
    let value = ByteArray::from(vec![1, 2, 3, 4, 5]);
    let row = Row::convert_byte_array(
      PhysicalType::BYTE_ARRAY, LogicalType::NONE, value.clone());
    assert_eq!(row, Row::Bytes(value));

    // BSON
    let value = ByteArray::from(vec![1, 2, 3, 4, 5]);
    let row = Row::convert_byte_array(
      PhysicalType::BYTE_ARRAY, LogicalType::BSON, value.clone());
    assert_eq!(row, Row::Bytes(value));
  }

  #[test]
  fn test_row_display() {
    // Primitive types
    assert_eq!(format!("{}", Row::Null), "null");
    assert_eq!(format!("{}", Row::Bool(true)), "true");
    assert_eq!(format!("{}", Row::Bool(false)), "false");
    assert_eq!(format!("{}", Row::Byte(1)), "1");
    assert_eq!(format!("{}", Row::Short(2)), "2");
    assert_eq!(format!("{}", Row::Int(3)), "3");
    assert_eq!(format!("{}", Row::Long(4)), "4");
    assert_eq!(format!("{}", Row::Float(5.0)), "5.0");
    assert_eq!(format!("{}", Row::Float(5.1234)), "5.1234");
    assert_eq!(format!("{}", Row::Double(6.0)), "6.0");
    assert_eq!(format!("{}", Row::Double(6.1234)), "6.1234");
    assert_eq!(format!("{}", Row::Str("abc".to_string())), "\"abc\"");
    assert_eq!(format!("{}", Row::Bytes(ByteArray::from(vec![1, 2, 3]))), "[1, 2, 3]");
    assert_eq!(format!("{}", Row::Timestamp(12345678)), "12345678");

    // Complex types
    let row = Row::Group(vec![
      ("x".to_string(), Row::Null),
      ("Y".to_string(), Row::Int(2)),
      ("z".to_string(), Row::Float(3.1)),
      ("a".to_string(), Row::Str("abc".to_string()))
    ]);
    assert_eq!(format!("{}", row), "{x: null, Y: 2, z: 3.1, a: \"abc\"}");

    let row = Row::List(vec![
      Row::Int(2),
      Row::Int(1),
      Row::Null,
      Row::Int(12)
    ]);
    assert_eq!(format!("{}", row), "[2, 1, null, 12]");

    let row = Row::Map(vec![
      (Row::Int(1), Row::Float(1.2)),
      (Row::Int(2), Row::Float(4.5)),
      (Row::Int(3), Row::Float(2.3))
    ]);
    assert_eq!(format!("{}", row), "{1 -> 1.2, 2 -> 4.5, 3 -> 2.3}");
  }
}
