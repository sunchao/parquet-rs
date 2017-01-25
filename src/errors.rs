use std::result;

quick_error! {
  #[derive(Debug)]
  pub enum ParquetError {
    LogicalTypeInit(msg: String) {
      display("Logical type init error: {}", msg)
      description(msg)
    }
  }
}

pub type Result<T> = result::Result<T, ParquetError>;
