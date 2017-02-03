use std::io;
use std::result;

quick_error! {
  #[derive(Debug)]
  pub enum ParquetError {
    Parse(message: String) {
      from()
      display("{}", message)
      description(message)
    }
    Io(err: io::Error) {
      from()
    }
  }
}

pub type Result<T> = result::Result<T, ParquetError>;

/// A convenient macro to create Parquet errors
macro_rules! err {
  ($fmt:expr) => (Err(ParquetError::Parse(format!($fmt))));
  ($fmt:expr, $($args:tt),*) => (Err(ParquetError::Parse(format!($fmt, $($args),*))));
}
