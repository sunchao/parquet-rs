extern crate parquet_rs;

use std::env;
use std::process;
use std::fs::File;
use std::path::Path;
use std::error::Error;
use std::io::BufReader;

use parquet_rs::file::reader::{ParquetFileInfo, ParquetFileReader};

fn main() {
  let args: Vec<_> = env::args().collect();
  if args.len() != 2 {
    println!("Usage: dump-schema <file-path>");
    process::exit(1);
  }
  let path = Path::new(&args[1]);
  let file = match File::open(&path) {
    Err(e) => {
      panic!("Error when opening file {}: {}", path.display(), Error::description(&e));
    },
    Ok(f) => f
  };
  let buf = BufReader::new(file);
  let mut parquet_reader = ParquetFileReader::new(buf);
  let _ = parquet_reader.metadata();
  println!("Done");
}
