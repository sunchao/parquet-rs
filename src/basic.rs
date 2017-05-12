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
use std::convert;
use parquet_thrift::parquet;


// ----------------------------------------------------------------------
// Types from the Thrift definition


/// Mirrors `parquet::Type`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Type {
  BOOLEAN,
  INT32,
  INT64,
  INT96,
  FLOAT,
  DOUBLE,
  BYTE_ARRAY,
  FIXED_LEN_BYTE_ARRAY
}

/// Mirrors `parquet::ConvertedType`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogicalType {
  NONE,
  UTF8,
  MAP,
  MAP_KEY_VALUE,
  LIST,
  ENUM,
  DECIMAL,
  DATE,
  TIME_MILLIS,
  TIME_MICROS,
  TIMESTAMP_MILLIS,
  TIMESTAMP_MICROS,
  UINT_8,
  UINT_16,
  UINT_32,
  UINT_64,
  INT_8,
  INT_16,
  INT_32,
  INT_64,
  JSON,
  BSON,
  INTERVAL
}

/// Mirrors `parquet::FieldRepetitionType`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Repetition {
  REQUIRED,
  OPTIONAL,
  REPEATED
}

/// Mirrors `parquet::Encoding`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Encoding {
  PLAIN,
  PLAIN_DICTIONARY,
  RLE,
  BIT_PACKED,
  DELTA_BINARY_PACKED,
  DELTA_LENGTH_BYTE_ARRAY,
  DELTA_BYTE_ARRAY,
  RLE_DICTIONARY
}

/// Mirrors `parquet::CompressionCodec`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Compression {
  UNCOMPRESSED,
  SNAPPY,
  GZIP,
  LZO,
  BROTLI
}

/// Mirrors `parquet::PageType`
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PageType {
  DATA_PAGE,
  INDEX_PAGE,
  DICTIONARY_PAGE,
  DATA_PAGE_V2
}

impl fmt::Display for Type {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl fmt::Display for LogicalType {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl fmt::Display for Repetition {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl fmt::Display for Encoding {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl fmt::Display for Compression {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl fmt::Display for PageType {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self)
  }
}

impl convert::From<parquet::Type> for Type {
  fn from(tp: parquet::Type) -> Self {
    match tp {
      parquet::Type::BOOLEAN => Type::BOOLEAN,
      parquet::Type::INT32 => Type::INT32,
      parquet::Type::INT64 => Type::INT64,
      parquet::Type::INT96 => Type::INT96,
      parquet::Type::FLOAT => Type::FLOAT,
      parquet::Type::DOUBLE => Type::DOUBLE,
      parquet::Type::BYTE_ARRAY => Type::BYTE_ARRAY,
      parquet::Type::FIXED_LEN_BYTE_ARRAY => Type::FIXED_LEN_BYTE_ARRAY
    }
  }
}

impl convert::From<Option<parquet::ConvertedType>> for LogicalType {
  fn from(op: Option<parquet::ConvertedType>) -> Self {
    match op {
      None => LogicalType::NONE,
      Some(tp) => {
        match tp {
          parquet::ConvertedType::UTF8 => LogicalType::UTF8,
          parquet::ConvertedType::MAP => LogicalType::MAP,
          parquet::ConvertedType::MAP_KEY_VALUE => LogicalType::MAP_KEY_VALUE,
          parquet::ConvertedType::LIST => LogicalType::LIST,
          parquet::ConvertedType::ENUM => LogicalType::ENUM,
          parquet::ConvertedType::DECIMAL => LogicalType::DECIMAL,
          parquet::ConvertedType::DATE => LogicalType::DATE,
          parquet::ConvertedType::TIME_MILLIS => LogicalType::TIME_MILLIS,
          parquet::ConvertedType::TIME_MICROS => LogicalType::TIME_MICROS,
          parquet::ConvertedType::TIMESTAMP_MILLIS => LogicalType::TIMESTAMP_MILLIS,
          parquet::ConvertedType::TIMESTAMP_MICROS => LogicalType::TIMESTAMP_MICROS,
          parquet::ConvertedType::UINT_8 => LogicalType::UINT_8,
          parquet::ConvertedType::UINT_16 => LogicalType::UINT_16,
          parquet::ConvertedType::UINT_32 => LogicalType::UINT_32,
          parquet::ConvertedType::UINT_64 => LogicalType::UINT_64,
          parquet::ConvertedType::INT_8 => LogicalType::INT_8,
          parquet::ConvertedType::INT_16 => LogicalType::INT_16,
          parquet::ConvertedType::INT_32 => LogicalType::INT_32,
          parquet::ConvertedType::INT_64 => LogicalType::INT_64,
          parquet::ConvertedType::JSON => LogicalType::JSON,
          parquet::ConvertedType::BSON => LogicalType::BSON,
          parquet::ConvertedType::INTERVAL => LogicalType::INTERVAL
        }
      }
    }
  }
}

impl convert::From<parquet::FieldRepetitionType> for Repetition {
  fn from(tp: parquet::FieldRepetitionType) -> Self {
    match tp {
      parquet::FieldRepetitionType::REQUIRED => Repetition::REQUIRED,
      parquet::FieldRepetitionType::OPTIONAL => Repetition::OPTIONAL,
      parquet::FieldRepetitionType::REPEATED => Repetition::REPEATED
    }
  }
}

impl convert::From<parquet::Encoding> for Encoding {
  fn from(tp: parquet::Encoding) -> Self {
    match tp {
      parquet::Encoding::PLAIN => Encoding::PLAIN,
      parquet::Encoding::PLAIN_DICTIONARY => Encoding::PLAIN_DICTIONARY,
      parquet::Encoding::RLE => Encoding::RLE,
      parquet::Encoding::BIT_PACKED => Encoding::BIT_PACKED,
      parquet::Encoding::DELTA_BINARY_PACKED => Encoding::DELTA_BINARY_PACKED,
      parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY => Encoding::DELTA_LENGTH_BYTE_ARRAY,
      parquet::Encoding::DELTA_BYTE_ARRAY => Encoding::DELTA_BYTE_ARRAY,
      parquet::Encoding::RLE_DICTIONARY => Encoding::RLE_DICTIONARY
    }
  }
}

impl convert::From<parquet::CompressionCodec> for Compression {
  fn from(tp: parquet::CompressionCodec) -> Self {
    match tp {
      parquet::CompressionCodec::UNCOMPRESSED => Compression::UNCOMPRESSED,
      parquet::CompressionCodec::SNAPPY => Compression::SNAPPY,
      parquet::CompressionCodec::GZIP => Compression::GZIP,
      parquet::CompressionCodec::LZO => Compression::LZO,
      parquet::CompressionCodec::BROTLI => Compression::BROTLI
    }
  }
}

impl convert::From<parquet::PageType> for PageType {
  fn from(tp: parquet::PageType) -> Self {
    match tp {
      parquet::PageType::DATA_PAGE => PageType::DATA_PAGE,
      parquet::PageType::INDEX_PAGE => PageType::INDEX_PAGE,
      parquet::PageType::DICTIONARY_PAGE => PageType::DICTIONARY_PAGE,
      parquet::PageType::DATA_PAGE_V2 => PageType::DATA_PAGE_V2
    }
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_display_type() {
    assert_eq!(Type::BOOLEAN.to_string(), "BOOLEAN");
    assert_eq!(Type::INT32.to_string(), "INT32");
    assert_eq!(Type::INT64.to_string(), "INT64");
    assert_eq!(Type::INT96.to_string(), "INT96");
    assert_eq!(Type::FLOAT.to_string(), "FLOAT");
    assert_eq!(Type::DOUBLE.to_string(), "DOUBLE");
    assert_eq!(Type::BYTE_ARRAY.to_string(), "BYTE_ARRAY");
    assert_eq!(Type::FIXED_LEN_BYTE_ARRAY.to_string(), "FIXED_LEN_BYTE_ARRAY");
  }

  #[test]
  fn test_from_type() {
    assert_eq!(Type::from(parquet::Type::BOOLEAN) , Type::BOOLEAN);
    assert_eq!(Type::from(parquet::Type::INT32) , Type::INT32);
    assert_eq!(Type::from(parquet::Type::INT64) , Type::INT64);
    assert_eq!(Type::from(parquet::Type::INT96) , Type::INT96);
    assert_eq!(Type::from(parquet::Type::FLOAT) , Type::FLOAT);
    assert_eq!(Type::from(parquet::Type::DOUBLE) , Type::DOUBLE);
    assert_eq!(Type::from(parquet::Type::BYTE_ARRAY) , Type::BYTE_ARRAY);
    assert_eq!(Type::from(parquet::Type::FIXED_LEN_BYTE_ARRAY) , Type::FIXED_LEN_BYTE_ARRAY);
  }

  #[test]
  fn test_display_logical_type() {
    assert_eq!(LogicalType::NONE.to_string(), "NONE");
    assert_eq!(LogicalType::UTF8.to_string(), "UTF8");
    assert_eq!(LogicalType::MAP.to_string(), "MAP");
    assert_eq!(LogicalType::MAP_KEY_VALUE.to_string(), "MAP_KEY_VALUE");
    assert_eq!(LogicalType::LIST.to_string(), "LIST");
    assert_eq!(LogicalType::ENUM.to_string(), "ENUM");
    assert_eq!(LogicalType::DECIMAL.to_string(), "DECIMAL");
    assert_eq!(LogicalType::DATE.to_string(), "DATE");
    assert_eq!(LogicalType::TIME_MILLIS.to_string(), "TIME_MILLIS");
    assert_eq!(LogicalType::DATE.to_string(), "DATE");
    assert_eq!(LogicalType::TIME_MICROS.to_string(), "TIME_MICROS");
    assert_eq!(LogicalType::TIMESTAMP_MILLIS.to_string(), "TIMESTAMP_MILLIS");
    assert_eq!(LogicalType::TIMESTAMP_MICROS.to_string(), "TIMESTAMP_MICROS");
    assert_eq!(LogicalType::UINT_8.to_string(), "UINT_8");
    assert_eq!(LogicalType::UINT_16.to_string(), "UINT_16");
    assert_eq!(LogicalType::UINT_32.to_string(), "UINT_32");
    assert_eq!(LogicalType::UINT_64.to_string(), "UINT_64");
    assert_eq!(LogicalType::INT_8.to_string(), "INT_8");
    assert_eq!(LogicalType::INT_16.to_string(), "INT_16");
    assert_eq!(LogicalType::INT_32.to_string(), "INT_32");
    assert_eq!(LogicalType::INT_64.to_string(), "INT_64");
    assert_eq!(LogicalType::JSON.to_string(), "JSON");
    assert_eq!(LogicalType::BSON.to_string(), "BSON");
    assert_eq!(LogicalType::INTERVAL.to_string(), "INTERVAL");
  }

    #[test]
  fn test_from_logical_type() {
    assert_eq!(LogicalType::from(None), LogicalType::NONE);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::UTF8)),
               LogicalType::UTF8);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::MAP)),
               LogicalType::MAP);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::MAP_KEY_VALUE)),
               LogicalType::MAP_KEY_VALUE);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::LIST)),
               LogicalType::LIST);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::ENUM)),
               LogicalType::ENUM);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::DECIMAL)),
               LogicalType::DECIMAL);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::DATE)),
               LogicalType::DATE);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::TIME_MILLIS)),
               LogicalType::TIME_MILLIS);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::TIME_MICROS)),
               LogicalType::TIME_MICROS);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::TIMESTAMP_MILLIS)),
               LogicalType::TIMESTAMP_MILLIS);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::TIMESTAMP_MICROS)),
               LogicalType::TIMESTAMP_MICROS);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::UINT_8)),
               LogicalType::UINT_8);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::UINT_16)),
               LogicalType::UINT_16);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::UINT_32)),
               LogicalType::UINT_32);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::UINT_64)),
               LogicalType::UINT_64);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::INT_8)),
               LogicalType::INT_8);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::INT_16)),
               LogicalType::INT_16);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::INT_32)),
               LogicalType::INT_32);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::INT_64)),
               LogicalType::INT_64);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::JSON)),
               LogicalType::JSON);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::BSON)),
               LogicalType::BSON);
    assert_eq!(LogicalType::from(Some(parquet::ConvertedType::INTERVAL)),
               LogicalType::INTERVAL);
  }

  #[test]
  fn test_display_repetition() {
    assert_eq!(Repetition::REQUIRED.to_string(), "REQUIRED");
    assert_eq!(Repetition::OPTIONAL.to_string(), "OPTIONAL");
    assert_eq!(Repetition::REPEATED.to_string(), "REPEATED");
  }

  #[test]
  fn test_from_repetition() {
    assert_eq!(Repetition::from(parquet::FieldRepetitionType::REQUIRED),
               Repetition::REQUIRED);
    assert_eq!(Repetition::from(parquet::FieldRepetitionType::OPTIONAL),
               Repetition::OPTIONAL);
    assert_eq!(Repetition::from(parquet::FieldRepetitionType::REPEATED),
               Repetition::REPEATED);
  }

  #[test]
  fn test_display_encoding() {
    assert_eq!(Encoding::PLAIN.to_string(), "PLAIN");
    assert_eq!(Encoding::PLAIN_DICTIONARY.to_string(), "PLAIN_DICTIONARY");
    assert_eq!(Encoding::RLE.to_string(), "RLE");
    assert_eq!(Encoding::BIT_PACKED.to_string(), "BIT_PACKED");
    assert_eq!(Encoding::DELTA_BINARY_PACKED.to_string(), "DELTA_BINARY_PACKED");
    assert_eq!(Encoding::DELTA_LENGTH_BYTE_ARRAY.to_string(), "DELTA_LENGTH_BYTE_ARRAY");
    assert_eq!(Encoding::DELTA_BYTE_ARRAY.to_string(), "DELTA_BYTE_ARRAY");
    assert_eq!(Encoding::RLE_DICTIONARY.to_string(), "RLE_DICTIONARY");
  }

  #[test]
  fn test_from_encoding() {
    assert_eq!(Encoding::from(parquet::Encoding::PLAIN),
               Encoding::PLAIN);
    assert_eq!(Encoding::from(parquet::Encoding::PLAIN_DICTIONARY),
               Encoding::PLAIN_DICTIONARY);
    assert_eq!(Encoding::from(parquet::Encoding::RLE), Encoding::RLE);
    assert_eq!(Encoding::from(parquet::Encoding::BIT_PACKED), Encoding::BIT_PACKED);
    assert_eq!(Encoding::from(parquet::Encoding::DELTA_BINARY_PACKED),
               Encoding::DELTA_BINARY_PACKED);
    assert_eq!(Encoding::from(parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY),
               Encoding::DELTA_LENGTH_BYTE_ARRAY);
    assert_eq!(Encoding::from(parquet::Encoding::DELTA_BYTE_ARRAY),
               Encoding::DELTA_BYTE_ARRAY);
  }

  #[test]
  fn test_display_compression() {
    assert_eq!(Compression::UNCOMPRESSED.to_string(), "UNCOMPRESSED");
    assert_eq!(Compression::SNAPPY.to_string(), "SNAPPY");
    assert_eq!(Compression::GZIP.to_string(), "GZIP");
    assert_eq!(Compression::LZO.to_string(), "LZO");
    assert_eq!(Compression::BROTLI.to_string(), "BROTLI");
  }

  #[test]
  fn test_from_compression() {
    assert_eq!(Compression::from(parquet::CompressionCodec::UNCOMPRESSED),
               Compression::UNCOMPRESSED);
    assert_eq!(Compression::from(parquet::CompressionCodec::SNAPPY),
               Compression::SNAPPY);
    assert_eq!(Compression::from(parquet::CompressionCodec::GZIP),
               Compression::GZIP);
    assert_eq!(Compression::from(parquet::CompressionCodec::LZO),
               Compression::LZO);
    assert_eq!(Compression::from(parquet::CompressionCodec::BROTLI),
               Compression::BROTLI);
  }

  #[test]
  fn test_display_page_type() {
    assert_eq!(PageType::DATA_PAGE.to_string(), "DATA_PAGE");
    assert_eq!(PageType::INDEX_PAGE.to_string(), "INDEX_PAGE");
    assert_eq!(PageType::DICTIONARY_PAGE.to_string(), "DICTIONARY_PAGE");
    assert_eq!(PageType::DATA_PAGE_V2.to_string(), "DATA_PAGE_V2");
  }

  #[test]
  fn test_from_page_type() {
    assert_eq!(PageType::from(parquet::PageType::DATA_PAGE), PageType::DATA_PAGE);
    assert_eq!(PageType::from(parquet::PageType::INDEX_PAGE), PageType::INDEX_PAGE);
    assert_eq!(PageType::from(parquet::PageType::DICTIONARY_PAGE), PageType::DICTIONARY_PAGE);
    assert_eq!(PageType::from(parquet::PageType::DATA_PAGE_V2), PageType::DATA_PAGE_V2);
  }
}
