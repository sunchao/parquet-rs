use basic::{Encoding, Type};
use schema::types::Type as SchemaType;

pub struct ParquetMetaData {
  file_metadata: FileMetaData,
  row_groups: Vec<RowGroupMetaData>
}

/// Metadata for a Parquet file
pub struct FileMetaData {
  version: i32,
  num_rows: i64,
  created_by: Option<String>,
  schema: Vec<Box<SchemaType>>
}

impl FileMetaData {
  pub fn new(version: i32, num_rows: i64, created_by: Option<String>,
             schema: Vec<Box<SchemaType>>) -> Self {
    FileMetaData { version, num_rows, created_by, schema }
  }

  pub fn version(&self) -> i32 {
    self.version
  }

  pub fn num_rows(&self) -> i64 {
    self.num_rows
  }

  pub fn created_by(&self) -> &Option<String> {
    &self.created_by
  }
}

/// Metadata for a row group
pub struct RowGroupMetaData {
  columns: Vec<Box<ColumnChunkMetaData>>,
  num_rows: i64,
  total_byte_size: i64,
  path: String
}

// Metadata for a column chunk
pub struct ColumnChunkMetaData {
  column_type: Type,
  column_path: ColumnPath,
  encodings: Vec<Encoding>,
  file_path: Option<String>,
  file_offset: i64,
  num_values: i64,
  total_compressed_size: i64,
  total_uncompressed_size: i64,
  data_page_offset: i64,
  index_page_offset: Option<i64>,
  dictionary_page_offset: Option<i64>
}

/// Represents common operations for a column chunk
impl ColumnChunkMetaData {
  /// File where the column chunk is stored. If not set, assumed to
  /// be at the same file as the metadata.
  /// This path is relative to the current file.
  pub fn file_path(&self) -> &Option<String> {
    &self.file_path
  }

  /// Byte offset in `file_path()`.
  pub fn file_offset(&self) -> i64 {
    self.file_offset
  }

  /// Type of this column. Must be primitive.
  pub fn column_type(&self) -> Type {
    self.column_type
  }

  /// Path (or identifier) of this column
  pub fn column_path(&self) -> &ColumnPath {
    &self.column_path
  }

  /// All encodings used for this column
  pub fn encodings(&self) -> &Vec<Encoding> {
    &self.encodings
  }

  /// TODO: add codec

  /// Total number of values in this column chunk
  pub fn num_values(&self) -> i64 {
    self.num_values
  }

  /// Get the total compressed data size of this column chunk
  fn compressed_size(&self) -> i64 {
    self.total_compressed_size
  }

  /// Get the total uncompressed data size of this column chunk
  fn uncompressed_size(&self) -> i64 {
    self.total_uncompressed_size
  }

  /// Get the offset for the column data
  fn data_page_offset(&self) -> i64 {
    self.data_page_offset
  }

  /// Whether this column chunk contains a dictionary page
  fn has_dictionary_page(&self) -> bool {
    self.dictionary_page_offset.is_some()
  }

  /// TODO: add statistics

  /// Get the offset for the dictionary page, if any
  fn dictionary_page_offset(&self) -> Option<i64> {
    self.dictionary_page_offset.clone()
  }
}

/// Represents a path in a nested schema
pub struct ColumnPath {
  parts: Vec<String>
}

impl ColumnPath {
  pub fn new(parts: Vec<String>) -> Self {
    ColumnPath { parts: parts }
  }
}
