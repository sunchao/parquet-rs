use std::fmt;

/// Mirrors parquet::Type
#[derive(Debug)]
enum Type {
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
#[derive(Debug)]
enum LogicalType {
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
#[derive(Debug)]
enum Repetition {
  REQUIRED,
  OPTIONAL,
  REPEATED
}

/// Mirrors parquet::Encoding
#[derive(Debug)]
enum Encoding {
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
#[derive(Debug)]
enum Compression {
  UNCOMPRESSED,
  SNAPPY,
  GZIP,
  LZO,
  BROTLI
}

/// Mirrors parquet::PageType
#[derive(Debug)]
enum PageType {
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
