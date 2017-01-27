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
