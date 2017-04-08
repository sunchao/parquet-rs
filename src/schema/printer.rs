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

use std::fmt;
use std::io;

use basic::LogicalType;
use schema::types::Type;
use file::metadata::{ParquetMetaData, FileMetaData, RowGroupMetaData, ColumnChunkMetaData};

/// Print Parquet metadata
#[allow(unused_must_use)]
pub fn print_parquet_metadata(out: &mut io::Write, metadata: &ParquetMetaData) {
  print_file_metadata(out, metadata.file_metadata());
  writeln!(out, "");
  writeln!(out, "");
  writeln!(out, "num of row groups: {}", metadata.num_row_groups());
  writeln!(out, "row groups:");
  writeln!(out, "");
  for (i, rg) in metadata.row_groups().iter().enumerate() {
    writeln!(out, "row group {}:", i);
    print_dashes(out, 80);
    print_row_group_metadata(out, rg);
  }
}

/// Print metadata information from `file_metadata`.
#[allow(unused_must_use)]
pub fn print_file_metadata(out: &mut io::Write, file_metadata: &FileMetaData) {
  writeln!(out, "version: {}", file_metadata.version());
  writeln!(out, "num of rows: {}", file_metadata.num_rows());
  if let Some(created_by) = file_metadata.created_by().as_ref() {
    writeln!(out, "created by: {}", created_by);
  }
  let schema = file_metadata.schema();
  print_schema(out, schema);
}

#[allow(unused_must_use)]
pub fn print_schema(out: &mut io::Write, tp: &Type) {
  // TODO: better if we can pass fmt::Write to Printer.
  // But how can we make it to accept both io::Write & fmt::Write?
  let mut s = String::new();
  {
    let mut printer = Printer::new(&mut s);
    printer.print(tp);
  }
  write!(out, "{}", s);
}

#[allow(unused_must_use)]
fn print_row_group_metadata(out: &mut io::Write, rg_metadata: &RowGroupMetaData) {
  writeln!(out, "total byte size: {}", rg_metadata.total_byte_size());
  writeln!(out, "num of rows: {}", rg_metadata.num_rows());
  writeln!(out, "");
  writeln!(out, "num of columns: {}", rg_metadata.num_columns());
  writeln!(out, "columns: ");
  for (i, cc) in rg_metadata.columns().iter().enumerate() {
    writeln!(out, "");
    writeln!(out, "column {}:", i);
    print_dashes(out, 80);
    print_column_chunk_metadata(out, cc);
  }
}

#[allow(unused_must_use)]
fn print_column_chunk_metadata(out: &mut io::Write, cc_metadata: &ColumnChunkMetaData) {
  writeln!(out, "column type: {}", cc_metadata.column_type());
  writeln!(out, "column path: {}", cc_metadata.column_path());
  let encoding_strs: Vec<_> = cc_metadata.encodings().iter()
    .map(|e| format!("{}", e)).collect();
  writeln!(out, "encodings: {}", encoding_strs.join(" "));
  let file_path_str = match cc_metadata.file_path() {
    None => "N/A",
    Some(ref fp) => *fp
  };
  writeln!(out, "file path: {}", file_path_str);
  writeln!(out, "file offset: {}", cc_metadata.file_offset());
  writeln!(out, "num of values: {}", cc_metadata.num_values());
  writeln!(out, "total compressed size (in bytes): {}", cc_metadata.compressed_size());
  writeln!(out, "total uncompressed size (in bytes): {}", cc_metadata.uncompressed_size());
  writeln!(out, "data page offset: {}", cc_metadata.data_page_offset());
  let index_page_offset_str = match cc_metadata.index_page_offset() {
    None => "N/A".to_owned(),
    Some(ipo) => ipo.to_string()
  };
  writeln!(out, "index page offset: {}", index_page_offset_str);
  let dict_page_offset_str = match cc_metadata.dictionary_page_offset() {
    None => "N/A".to_owned(),
    Some(dpo) => dpo.to_string()
  };
  writeln!(out, "dictionary page offset: {}", dict_page_offset_str);
  writeln!(out, "");
}

#[allow(unused_must_use)]
fn print_dashes(out: &mut io::Write, num: i32) {
  for _ in 0..num {
    write!(out, "-");
  }
  writeln!(out, "");
}

const INDENT_WIDTH: i32 = 2;

pub struct Printer<'a> {
  output: &'a mut fmt::Write,
  indent: i32
}

#[allow(unused_must_use)]
impl <'a> Printer<'a> {
  fn new(output: &'a mut fmt::Write) -> Self {
    Printer { output: output, indent: 0 }
  }

  fn print_indent(&mut self) {
    for _ in 0..self.indent {
      write!(self.output, " ");
    }
  }
}

#[allow(unused_must_use)]
impl<'a> Printer<'a> {
  pub fn print(&mut self, tp: &Type) {
    self.print_indent();
    match tp {
      &Type::PrimitiveType{ ref basic_info, physical_type, .. } => {
        write!(self.output, "{} {} {};", basic_info.repetition(), physical_type, basic_info.name());
      },
      &Type::GroupType{ ref basic_info, ref fields } => {
        if basic_info.has_repetition() {
          let r = basic_info.repetition();
          write!(self.output, "{} group {} ", r, basic_info.name());
          if basic_info.logical_type() != LogicalType::NONE {
            write!(self.output, "({}) ", basic_info.logical_type());
          }
          writeln!(self.output, "{{");
        } else {
          writeln!(self.output, "message {} {{", basic_info.name());
        }

        self.indent += INDENT_WIDTH;
        for c in fields {
          self.print(&c);
          writeln!(self.output, "");
        }
        self.indent -= INDENT_WIDTH;
        self.print_indent();
        write!(self.output, "}}");
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use schema::types::Type;
  use basic::{Type as PhysicalType, Repetition};

  #[test]
  fn test_print_primitive_type() {
    let mut s = String::new();
    {
      let mut p = Printer::new(&mut s);
      let foo = Type::new_primitive_type(
        "foo", Repetition::REQUIRED, PhysicalType::INT32,
        LogicalType::INT_32, 0, 0, 0, None).unwrap();
      p.print(&foo);
    }
    assert_eq!(&mut s, "REQUIRED INT32 foo;");
  }

  #[test]
  fn test_print_group_type() {
    let mut s = String::new();
    {
      let mut p = Printer::new(&mut s);
      let f1 = Type::new_primitive_type(
        "f1", Repetition::REQUIRED, PhysicalType::INT32,
        LogicalType::INT_32, 0, 0, 0, Some(0));
      let f2 = Type::new_primitive_type(
        "f2", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
        LogicalType::UTF8, 0, 0, 0, Some(1));
      let f3 = Type::new_primitive_type(
        "f3", Repetition::REPEATED, PhysicalType::FIXED_LEN_BYTE_ARRAY,
        LogicalType::INTERVAL, 12, 0, 0, Some(2));
      let mut struct_fields = Vec::new();
      struct_fields.push(f1.unwrap());
      struct_fields.push(f2.unwrap());
      let foo = Type::new_group_type(
        "foo", Some(Repetition::OPTIONAL), LogicalType::NONE, struct_fields, Some(1)).unwrap();
      let mut fields = Vec::new();
      fields.push(foo);
      fields.push(f3.unwrap());
      let message = Type::new_group_type(
        "schema", None, LogicalType::NONE, fields, Some(2)).unwrap();
      p.print(&message);
    }
    let expected =
"message schema {
  OPTIONAL group foo {
    REQUIRED INT32 f1;
    OPTIONAL BYTE_ARRAY f2;
  }
  REPEATED FIXED_LEN_BYTE_ARRAY f3;
}";
    assert_eq!(&mut s, expected);
  }
}
