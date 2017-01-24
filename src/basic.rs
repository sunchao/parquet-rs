use std::fmt;

/// Mirrors parquet::Type
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

/// Mirrors parquet::ConvertedType
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

/// Mirrors parquet::FieldRepetitionType
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Repetition {
  REQUIRED,
  OPTIONAL,
  REPEATED
}

/// Mirrors parquet::Encoding
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

/// Mirrors parquet::CompressionCodec
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Compression {
  UNCOMPRESSED,
  SNAPPY,
  GZIP,
  LZO,
  BROTLI
}

/// Mirrors parquet::PageType
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
  fn test_display_repetition() {
    assert_eq!(Repetition::REQUIRED.to_string(), "REQUIRED");
    assert_eq!(Repetition::OPTIONAL.to_string(), "OPTIONAL");
    assert_eq!(Repetition::REPEATED.to_string(), "REPEATED");
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
  fn test_display_compression() {
    assert_eq!(Compression::UNCOMPRESSED.to_string(), "UNCOMPRESSED");
    assert_eq!(Compression::SNAPPY.to_string(), "SNAPPY");
    assert_eq!(Compression::GZIP.to_string(), "GZIP");
    assert_eq!(Compression::LZO.to_string(), "LZO");
    assert_eq!(Compression::BROTLI.to_string(), "BROTLI");
  }

  #[test]
  fn test_display_page_type() {
    assert_eq!(PageType::DATA_PAGE.to_string(), "DATA_PAGE");
    assert_eq!(PageType::INDEX_PAGE.to_string(), "INDEX_PAGE");
    assert_eq!(PageType::DICTIONARY_PAGE.to_string(), "DICTIONARY_PAGE");
    assert_eq!(PageType::DATA_PAGE_V2.to_string(), "DATA_PAGE_V2");
  }
}
