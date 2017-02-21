// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

extern crate parquet_rs;

use std::env;
use std::process;
use std::fs::File;
use std::path::Path;
use std::error::Error;
use std::io::BufReader;

use parquet_rs::file::reader::{ParquetFileInfo, ParquetFileReader};
use parquet_rs::schema::printer::{print_parquet_metadata, print_file_metadata};

fn main() {
  let args: Vec<_> = env::args().collect();
  if args.len() != 2 && args.len() != 3 {
    println!("Usage: dump-schema <file-path> <verbose>");
    process::exit(1);
  }
  let path = Path::new(&args[1]);
  let mut verbose = false;
  if args.len() == 3 {
    match args[2].parse() {
      Ok(b) => verbose = b,
      Err(e) => panic!("Error when reading value for <verbose> \
                        (expected either 'true' or 'false'): {}", Error::description(&e))
    }
  }
  let file = match File::open(&path) {
    Err(e) => {
      panic!("Error when opening file {}: {}", path.display(), Error::description(&e));
    },
    Ok(f) => f
  };
  let buf = BufReader::new(file);
  let mut parquet_reader = ParquetFileReader::new(buf);
  let result = parquet_reader.metadata();
  match result {
    Ok(metadata) => {
      println!("Metadata for file: {}", &args[1]);
      println!("");
      if verbose {
        print_parquet_metadata(&mut std::io::stdout(), &metadata);
      } else {
        print_file_metadata(&mut std::io::stdout(), metadata.file_metadata());
      }
    },
    Err(e) => {
      println!("Error while dumping metadata. Error is: {}", e);
    }
  }
}
