use std::result;

quick_error! {
  #[derive(Debug)]
  pub enum ParquetError {
    Schema(msg: String) {
      display("Schema init error: {}", msg)
      description(msg)
    }
  }
}

pub type Result<T> = result::Result<T, ParquetError>;
