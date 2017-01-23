use basic::Encoding;

struct ParquetMetaData {
  file_metadata: FileMetaData,
  blocks: Vec<BlockMetaData>
}

struct FileMetaData {
  
}

struct BlockMetaData {
  // columns: Vec<ColumnChunkMetaData>,
  row_count: i64,
  total_byte_size: i64,
  path: String
}

/// Represents common operations for a column chunk
trait ColumnChunkMetaData {
  /// Get the total compressed data size of this column chunk
  fn compressed_size(&self) -> i64;

  /// Get the total uncompressed data size of this column chunk
  fn uncompressed_size(&self) -> i64;

  /// Whether this column chunk contains a dictionary page
  fn has_dictionary_page(&self) -> bool;

  /// Get the offset for the column data
  fn data_page_offset(&self) -> i64;

  /// Get the offset for the dictionary page, if any
  fn dictionary_page_offset(&self) -> Option<i64>;

  /// Get a set of encodings used in this column chunk
  fn encodings(&self) -> Vec<Encoding>;
}
