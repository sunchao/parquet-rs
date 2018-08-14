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

//! Low level column reader API.
//!
//! This API is designed for the direct mapping with subsequent manual handling of
//! definition and repetition levels and spacing. This allows to create column vectors
//! and batches and map them directly to Parquet data.
//!
//! See below an example of using the API.
//!
//! # Example
//!
//! ```rust
//! use std::fs::File;
//! use std::path::Path;
//!
//! use parquet::basic::Type;
//! use parquet::data_type::Int32Type;
//! use parquet::column::reader::get_typed_column_reader;
//! use parquet::file::reader::{FileReader, SerializedFileReader};
//!
//! // Open Parquet file and initialize reader
//! let path = Path::new("data/alltypes_plain.parquet");
//! let file = File::open(&path).unwrap();
//! let parquet_reader = SerializedFileReader::new(file).unwrap();
//! let metadata = parquet_reader.metadata();
//!
//! for i in 0..metadata.num_row_groups() {
//!   let row_group_reader = parquet_reader.get_row_group(i).unwrap();
//!   let row_group_metadata = metadata.row_group(i);
//!
//!   for j in 0..row_group_metadata.num_columns() {
//!     let column = row_group_metadata.column(j);
//!
//!     // Extract column reader and map to typed column reader for required columns.
//!     let column_reader = row_group_reader
//!       .get_column_reader(j)
//!       .expect("Valid column reader");
//!
//!     // Extract typed column reader for any INT32 column in the file.
//!     // It is also possible to extract certain columns based on column descriptors
//!     // from metadata.
//!
//!     match column.column_type() {
//!       Type::INT32 => {
//!         let mut typed_column_reader =
//!           get_typed_column_reader::<Int32Type>(column_reader);
//!
//!         // See `read_batch` method for comments on different parameters.
//!         let mut values = vec![0; 16];
//!         let mut def_levels = vec![0; 16];
//!         let mut rep_levels = vec![0; 16];
//!
//!         let num_values = typed_column_reader.read_batch(
//!           8, // batch size
//!           Some(&mut def_levels), // definition levels
//!           Some(&mut rep_levels), // repetition levels
//!           &mut values // read values
//!         );
//!
//!         println!(
//!           "Read {:?} values, values: {:?}, def_levels: {:?}, rep_levels: {:?}",
//!           num_values,
//!           values,
//!           def_levels,
//!           rep_levels,
//!         );
//!       },
//!       _ => {
//!         // Skip any other columns for now, but there could be similar processing.
//!         println!(
//!           "Skipped column {} of type {}",
//!           column.column_path().string(),
//!           column.column_type()
//!         );
//!       }
//!     }
//!   }
//! }
//! ```

pub mod page;
pub mod reader;
pub mod writer;
