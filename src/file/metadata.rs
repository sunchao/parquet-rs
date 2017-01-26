use basic::Encoding;
use schema::types::Type;

struct ParquetMetaData {
  file_metadata: FileMetaData,
  blocks: Vec<BlockMetaData>
}

struct FileMetaData {

}

struct BlockMetaData {
  columns: Vec<Box<ColumnChunkMetaData>>,
  row_count: i64,
  total_byte_size: i64,
  path: String
}

/// Represents common operations for a column chunk
trait ColumnChunkMetaData {
  /// File where the column chunk is stored. If not set, assumed to
  /// be at the same file as the metadata.
  /// This path is relative to the current file.
  fn file_path(&self) -> Option<String>;

  /// Byte offset in `file_path()`.
  fn file_offset(&self) -> i64;

  /// Type of this column
  fn column_type(&self) -> Type;

  /// All encodings used for this column
  fn encodings(&self) -> &Vec<Encoding>;

  /// TODO: add codec

  /// Total number of values in this column chunk
  fn num_values(&self) -> i64;

  /// Get the total compressed data size of this column chunk
  fn compressed_size(&self) -> i64;

  /// Get the total uncompressed data size of this column chunk
  fn uncompressed_size(&self) -> i64;

  /// Whether this column chunk contains a dictionary page
  fn has_dictionary_page(&self) -> bool;

  /// Get the offset for the column data
  fn data_page_offset(&self) -> i64;

  /// TODO: add statistics

  /// Get the offset for the dictionary page, if any
  fn dictionary_page_offset(&self) -> Option<i64>;
}
