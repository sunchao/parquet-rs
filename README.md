# parquet-rs

[![Build Status](https://travis-ci.org/sunchao/parquet-rs.svg?branch=master)](https://travis-ci.org/sunchao/parquet-rs)
[![Coverage Status](https://coveralls.io/repos/github/sunchao/parquet-rs/badge.svg?branch=master)](https://coveralls.io/github/sunchao/parquet-rs?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![](http://meritbadge.herokuapp.com/parquet)](https://crates.io/crates/parquet)
[![Documentation](https://docs.rs/parquet/badge.svg)](https://docs.rs/parquet)

An [Apache Parquet](https://parquet.apache.org/) implementation in Rust (work in progress)

## Usage
Add this to your Cargo.toml:
```toml
[dependencies]
parquet = "0.2"
```

and this to your crate root:
```rust
extern crate parquet;
```

Example usage:
```rust
use std::fs::File;
use std::path::Path;
use parquet::file::reader::{FileReader, SerializedFileReader};

let file = File::open(&Path::new("/path/to/file")).unwrap();
let reader = SerializedFileReader::new(file).unwrap();
let mut iter = reader.get_row_iter(None).unwrap();
while let Some(record) = iter.next() {
  println!("{}", record);
}
```
See crate documentation on available API.

## Supported Parquet Version
- Parquet-format 2.4.0

To update Parquet format to a newer version, check if [parquet-format](https://github.com/sunchao/parquet-format-rs)
version is available. Then simply update version of `parquet-format` crate in Cargo.toml.

## Requirements
- Rust nightly

See [Working with nightly Rust](https://github.com/rust-lang-nursery/rustup.rs/blob/master/README.md#working-with-nightly-rust)
to install nightly toolchain and set it as default.

## Build
Run `cargo build` or `cargo build --release` to build in release mode.

## Test
Run `cargo test` for unit tests.

## Binaries
The following binaries are provided (use `cargo install` to install them):
- **parquet-schema** for printing Parquet file schema and metadata.
`Usage: parquet-schema <file-path> [verbose]`, where `file-path` is the path to a Parquet file,
and optional `verbose` is the boolean flag that allows to print full metadata or schema only
(when not specified only schema will be printed).

- **parquet-read** for reading records from a Parquet file.
`Usage: parquet-read <file-path> [num-records]`, where `file-path` is the path to a Parquet file,
and `num-records` is the number of records to read from a file (when not specified all records will
be printed).

## Benchmarks
Run `cargo bench` for benchmarks.

## Docs
To build documentation, run `cargo doc --no-deps`.
To compile and view in the browser, run `cargo doc --no-deps --open`.

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.
