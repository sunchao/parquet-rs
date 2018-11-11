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
//! row groups or column chunks from a file, read and write records/values.
//!
//! # Usage
//!
//! See the link [crates.io/crates/parquet](https://crates.io/crates/parquet) for the
//! latest version of the crate.
//!
//! Add `parquet` to the list of dependencies in `Cargo.toml` and this to the project's
//! crate root:
//!
//! ```
//! extern crate parquet;
//! ```
//!
//! # Example
//!
//! Import file reader to get access to Parquet metadata, including the file schema.
//!
//! ```
//! #![feature(try_from)]
//!
//! use parquet::file::reader::{FileReader, SerializedFileReader};
//! use std::convert::TryFrom;
//!
//! let reader = SerializedFileReader::try_from("data/alltypes_plain.parquet").unwrap();
//!
//! let parquet_metadata = reader.metadata();
//! assert_eq!(parquet_metadata.num_row_groups(), 1);
//!
//! let file_metadata = parquet_metadata.file_metadata();
//! assert_eq!(file_metadata.num_rows(), 8);
//!
//! let schema = file_metadata.schema();
//! assert_eq!(schema.get_fields().len(), 11);
//! ```
//!
//! Crate provides several [read](#read-api) and [write](#write-api) API options. Below
//! is an example of using the record reader API.
//!
//! ```
//! #![feature(try_from)]
//!
//! use parquet::file::reader::{FileReader, SerializedFileReader};
//! use std::convert::TryFrom;
//!
//! let reader = SerializedFileReader::try_from("data/alltypes_plain.parquet").unwrap();
//!
//! // Reading data using record API with optional projection schema.
//! let mut iter = reader.get_row_iter(None).unwrap();
//! while let Some(record) = iter.next() {
//!   // See record API for different field accessors
//!   println!("{}", record);
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
//! # Statistics
//!
//! Statistics are optional, and provide min/max values, null count, etc. for each column
//! or data page, from which they could be accessed respectively, and are described in
//! [`statistics`](`file::statistics`) module.
//!
//! # Schema and type
//!
//! Parquet schema can be extracted from [`FileMetaData`](`file::metadata::FileMetaData`)
//! and is represented by Parquet type.
//!
//! Parquet type is described by [`Type`](`schema::types::Type`), including top level
//! message type (schema). Refer to the [`schema`] module for the detailed information
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
//! - Low level column reader API (see [`file`] and [`column`] modules)
//! - Arrow API (_TODO_)
//! - High level record API (see [`record`] module)
//!
//! # Write API
//!
//! Crate also provides API to write data in Parquet format:
//! - Low level column writer API (see [`file`] and [`column`] modules)
//! - Arrow API (_TODO_)
//! - High level API for writing records (_TODO_)

#![feature(type_ascription)]
#![feature(rustc_private)]
#![feature(specialization)]
#![feature(try_from)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]

#[macro_use]
extern crate quick_error;
extern crate arena;
extern crate brotli;
extern crate byteorder;
extern crate chrono;
extern crate flate2;
extern crate lz4;
extern crate num_bigint;
extern crate parquet_format;
extern crate snap;
extern crate thrift;
extern crate zstd;

#[cfg(test)]
extern crate rand;

#[macro_use]
pub mod errors;
pub mod basic;
pub mod data_type;

// Exported for external use, such as benchmarks
pub use encodings::{decoding, encoding};
pub use util::memory;

#[macro_use]
mod util;
pub mod column;
pub mod compression;
mod encodings;
pub mod file;
pub mod record;
pub mod schema;
