use std::result;

quick_error! {
  #[derive(Debug)]
  pub enum ParquetError {
    Error(message: String) {
      display("{}", message)
      description(message)
    }
  }
}

pub type Result<T> = result::Result<T, ParquetError>;

/// A convenient macro to create Parquet errors
macro_rules! err {
  ($fmt:expr) => (Err(ParquetError::Error(format!($fmt))));
  ($fmt:expr, $($args:tt),*) => (Err(ParquetError::Error(format!($fmt, $($args),*))));
}
