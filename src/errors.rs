use std::io;
use std::result;
use thrift;

quick_error! {
  #[derive(Debug)]
  pub enum ParquetError {
    Parse(message: String) {
      from()
      display("{}", message)
      description(message)
    }
    Io(message: String, err: io::Error) {
      from(err: io::Error) -> (format!("io error"), err)
      display("{}, underlying IO error: {}", message, err)
    }
    Thrift(message: String, err: thrift::Error) {
      from(err: thrift::Error) -> (format!("thrift error"), err)
      display("{}, underlying Thrift error: {}", message, err)
    }
  }
}

pub type Result<T> = result::Result<T, ParquetError>;

/// Convenient macros for different errors
macro_rules! parse_err {
  ($fmt:expr) => (ParquetError::Parse(format!($fmt)));
  ($fmt:expr, $($args:tt),*) => (ParquetError::Parse(format!($fmt, $($args),*)));
}

macro_rules! io_err {
  ($e:ident, $fmt:expr) => (ParquetError::Io(format!($fmt), $e));
  ($e:ident, $fmt:expr, $($args:tt),*) => (
    ParquetError::Io(&format!($fmt, $($args),*), $e));
}

macro_rules! thrift_err {
  ($e:ident, $fmt:expr) => (ParquetError::Thrift(format!($fmt), $e));
  ($e:ident, $fmt:expr, $($args:tt),*) => (
    ParquetError::Thrift(format!($fmt, $($args),*), $e));
}
