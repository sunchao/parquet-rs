use std::borrow::BorrowMut;

use basic::{Encoding, Type};
use errors::{Result, ParquetError};
use schema::types::Type as SchemaType;
use parquet_thrift::parquet::{ColumnChunk, ColumnMetaData, RowGroup};

pub struct ParquetMetaData {
  file_metadata: FileMetaData,
  row_groups: Vec<RowGroupMetaData>
}

/// Metadata for a Parquet file
pub struct FileMetaData {
  version: i32,
  num_rows: i64,
  created_by: Option<String>,
  schema: Box<SchemaType>
}

impl FileMetaData {
  pub fn new(version: i32, num_rows: i64, created_by: Option<String>,
             schema: Box<SchemaType>) -> Self {
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

  pub fn schema(&mut self) -> &mut SchemaType {
    self.schema.borrow_mut()
  }
}

/// Metadata for a row group
pub struct RowGroupMetaData {
  columns: Vec<ColumnChunkMetaData>,
  num_rows: i64,
  total_byte_size: i64
}

impl RowGroupMetaData {
  pub fn num_columns(&self) -> usize {
    self.columns.len()
  }

  pub fn column(&self, i: usize) -> &ColumnChunkMetaData {
    &self.columns[i]
  }

  pub fn num_rows(&self) -> i64 {
    self.num_rows
  }

  pub fn total_byte_size(&self) -> i64 {
    self.total_byte_size
  }

  pub fn from_thrift(rg: RowGroup) -> Result<RowGroupMetaData> {
    let total_byte_size = rg.total_byte_size;
    let num_rows = rg.num_rows;
    let mut columns = Vec::new();
    for c in rg.columns {
      columns.push(ColumnChunkMetaData::from_thrift(c)?)
    }
    Ok(RowGroupMetaData{columns, num_rows, total_byte_size})
  }
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
  pub fn compressed_size(&self) -> i64 {
    self.total_compressed_size
  }

  /// Get the total uncompressed data size of this column chunk
  pub fn uncompressed_size(&self) -> i64 {
    self.total_uncompressed_size
  }

  /// Get the offset for the column data
  pub fn data_page_offset(&self) -> i64 {
    self.data_page_offset
  }

  /// Whether this column chunk contains a dictionary page
  pub fn has_dictionary_page(&self) -> bool {
    self.dictionary_page_offset.is_some()
  }

  /// TODO: add statistics

  /// Get the offset for the dictionary page, if any
  pub fn dictionary_page_offset(&self) -> Option<i64> {
    self.dictionary_page_offset.clone()
  }

  /// Conversion from Thrift
  pub fn from_thrift(cc: ColumnChunk) -> Result<Self> {
    if cc.meta_data.is_none() {
      return Err(schema_err!("Expected to have column metadata"))
    }
    let mut col_metadata: ColumnMetaData = cc.meta_data.unwrap();
    let column_type = Type::from(col_metadata.type_);
    let column_path = ColumnPath::new(col_metadata.path_in_schema);
    let encodings = col_metadata.encodings.drain(0..).map(Encoding::from).collect();
    let file_path = cc.file_path;
    let file_offset = cc.file_offset;
    let num_values = col_metadata.num_values;
    let total_compressed_size = col_metadata.total_compressed_size;
    let total_uncompressed_size = col_metadata.total_uncompressed_size;
    let data_page_offset = col_metadata.data_page_offset;
    let index_page_offset = col_metadata.index_page_offset;
    let dictionary_page_offset = col_metadata.dictionary_page_offset;
    let result = ColumnChunkMetaData
    { column_type, column_path, encodings, file_path,
      file_offset, num_values, total_compressed_size, total_uncompressed_size,
      data_page_offset, index_page_offset, dictionary_page_offset };
    Ok(result)
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
