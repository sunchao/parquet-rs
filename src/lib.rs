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

//! [Apache Parquet](http://parquet.apache.org) is a columnar storage format that
//! provides efficient data compression and encoding schemes to improve performance of
//! handling complex nested data structures. Parquet implements record-shredding and
//! assembly algorithm described in the Dremel paper.
//!
//! Crate provides API to access file schema and metadata from a Parquet file, extract
//! row groups or column chunks from a file, and read records/values.
//!
//! # Usage
//!
//! This crate is [on crates.io](https://crates.io/crates/parquet) and can be used by
//! adding `parquet` to the list of dependencies in `Cargo.toml`.
//!
//! ```toml
//! [dependencies]
//! parquet = "0.1"
//! ```
//!
//! and this to the project's crate root:
//!
//! ```rust
//! extern crate parquet;
//! ```
//!
//! # Example
//!
//! Below is the example of reading a Parquet file, listing Parquet metadata including
//! column chunk metadata, using record API and accessing row group readers.
//!
//! ```rust
//! use std::fs::File;
//! use std::path::Path;
//! use parquet::file::reader::{FileReader, SerializedFileReader};
//!
//! // Creating a file reader
//! let path = Path::new("data/alltypes_plain.parquet");
//! let file = File::open(&path).expect("File should exist");
//! let reader = SerializedFileReader::new(file).expect("Valid Parquet file");
//!
//! // Listing Parquet metadata
//! let parquet_metadata = reader.metadata();
//! let file_metadata = parquet_metadata.file_metadata();
//! for i in 0..parquet_metadata.num_row_groups() {
//!   // Accessing row group metadata
//!   let row_group_metadata = parquet_metadata.row_group(i);
//!   // Accessing column chunk metadata
//!   for j in 0..row_group_metadata.num_columns() {
//!     let column_chunk_metadata = row_group_metadata.column(j);
//!   }
//! }
//!
//! // Reading data using record API
//! let mut iter = reader.get_row_iter(None).expect("Should be okay");
//! while let Some(record) = iter.next() {
//!   // do something with the record...
//!   println!("{}", record);
//! }
//!
//! // Accessing row group readers in a file
//! for i in 0..reader.num_row_groups() {
//!   let row_group_reader = reader.get_row_group(i).expect("Should be okay");
//! }
//! ```
//!
//! # Metadata
//!
//! Module [`metadata`](`file::metadata`) contains Parquet metadata structs, including
//! file metadata, that has information about file schema, version, and number of rows,
//! row group metadata with a set of column chunks that contain column type and encodings,
//! number of values and compressed/uncompressed size in bytes.
//!
//! # Schema and type
//!
//! Parquet schema can be extracted from [`FileMetaData`](`file::metadata::FileMetaData`)
//! and is represented by Parquet type.
//!
//! Parquet type is described by [`Type`](`schema::types::Type`), including top level
//! message type or schema. Refer to the [`schema`] module for the detailed information
//! on Type API, printing and parsing of message types.
//!
//! # File and row group API
//!
//! Module [`file`] contains all definitions to explore Parquet files metadata and data.
//! File reader [`FileReader`](`file::reader::FileReader`) is a starting point for
//! working with Parquet files - it provides set of methods to get file metadata, row
//! group readers [`RowGroupReader`](`file::reader::RowGroupReader`) to get access to
//! column readers and record iterator.
//!
//! # Read API
//!
//! Crate offers several methods to read data from a Parquet file:
//! - Low level column reader API (see [`column`] module)
//! - Arrow API (_TODO_)
//! - High level record API (see [`record`] module)
//!

#![feature(type_ascription)]
#![feature(rustc_private)]
#![feature(specialization)]
#![feature(cfg_target_feature)]
#![feature(match_default_bindings)]

#![allow(dead_code)]
#![allow(non_camel_case_types)]

#[macro_use]
extern crate quick_error;
extern crate byteorder;
extern crate thrift;
extern crate ordered_float;
extern crate try_from;
extern crate arena;
extern crate snap;
extern crate brotli;
extern crate flate2;
extern crate rand;
extern crate x86intrin;

#[macro_use]
pub mod errors;
pub mod basic;
pub mod data_type;
mod parquet_thrift;

// Exported for external use, such as benchmarks
pub use util::memory;
pub use encodings::encoding;
pub use encodings::decoding;

#[macro_use]
mod util;
mod encodings;
pub mod compression;
pub mod column;
pub mod record;
pub mod schema;
pub mod file;
