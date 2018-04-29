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

//! Main entrypoint for working with Parquet API.
//! Provides access to file and row group readers, record API, etc.
//!
//! See [`reader::SerializedFileReader`] for a starting reference and
//! [`metadata::ParquetMetaData`] for file metadata.
//!
//! # Example
//!
//! ```rust
//! use std::fs::File;
//! use std::path::Path;
//! use parquet::file::reader::{FileReader, SerializedFileReader};
//!
//! let path = Path::new("data/alltypes_plain.parquet");
//! let file = File::open(&path).unwrap();
//! let reader = SerializedFileReader::new(file).unwrap();
//!
//! let parquet_metadata = reader.metadata();
//! assert_eq!(parquet_metadata.num_row_groups(), 1);
//!
//! let row_group_reader = reader.get_row_group(0).unwrap();
//! assert_eq!(row_group_reader.num_columns(), 11);
//! ```

pub mod metadata;
pub mod reader;
