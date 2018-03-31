extern crate parquet;

use std::env;
use std::fs::File;
use std::path::Path;
use std::process;

use parquet::file::reader::{FileReader, SerializedFileReader};

fn main() {
  let args: Vec<String> = env::args().collect();
  if args.len() != 2 && args.len() != 3 {
    println!("Usage: parquet-read <file-path> [num-records]");
    process::exit(1);
  }

  let mut num_records: Option<usize> = None;
  if args.len() == 3 {
    match args[2].parse() {
      Ok(value) => num_records = Some(value),
      Err(e) => panic!("Error when reading value for [num-records], {}", e)
    }
  }

  let path = Path::new(&args[1]);
  let file = File::open(&path).unwrap();
  let parquet_reader = SerializedFileReader::new(file).unwrap();

  // Use full schema as projected schema
  let mut iter = parquet_reader.get_row_iter(None).unwrap();

  let mut start = 0;
  let end = num_records.unwrap_or(0);
  let all_records = num_records.is_none();

  while all_records || start < end {
    match iter.next() {
      Some(row) => println!("{}", row),
      None => break,
    }
    start += 1;
  }
}
