use std::fmt;
use std::io;

use basic::LogicalType;
use schema::types::{Type, PrimitiveType, GroupType, TypeVisitor};
use file::metadata::FileMetaData;

#[allow(unused_must_use)]
pub fn print_schema(out: &mut io::Write, tp: &mut Type) {
  // TODO: better if we can pass fmt::Write to Printer.
  // But how can we make it to accept both io::Write & fmt::Write?
  let mut s = String::new();
  {
    let mut printer = Printer::new(&mut s);
    tp.accept(&mut printer);
  }
  write!(out, "{}", s);
}

#[allow(unused_must_use)]
pub fn print_file_metadata(out: &mut io::Write, file_metadata: &mut FileMetaData) {
  writeln!(out, "version: {}", file_metadata.version());
  writeln!(out, "num of rows: {}", file_metadata.num_rows());
  if file_metadata.created_by().is_some() {
    writeln!(out, "created by: {}", file_metadata.created_by().as_ref().unwrap());
  }
  let schema = file_metadata.schema();
  print_schema(out, schema);
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
impl <'a> TypeVisitor for Printer<'a> {
  fn visit_primitive_type(&mut self, tp: &mut PrimitiveType) {
    self.print_indent();
    write!(self.output, "{} {} {};", tp.repetition().unwrap(), tp.physical_type(), tp.name());
  }

  fn visit_group_type(&mut self, tp: &mut GroupType) {
    self.print_indent();
    match tp.repetition() {
      None => {
        writeln!(self.output, "message {} {{", tp.name());
      },
      Some(r) => {
        write!(self.output, "{} group {} ", r, tp.name());
        if tp.logical_type() != LogicalType::NONE {
          write!(self.output, "({}) ", tp.logical_type());
        }
        writeln!(self.output, "{{");
      }
    }
    self.indent += INDENT_WIDTH;
    for c in tp.fields() {
      c.accept(self);
      writeln!(self.output, "");
    }
    self.indent -= INDENT_WIDTH;
    self.print_indent();
    write!(self.output, "}}");
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
      let mut foo = PrimitiveType::new(
        "foo", Repetition::REQUIRED, PhysicalType::INT32,
        LogicalType::INT_32, 0, 0, 0, None).unwrap();
      foo.accept(&mut p);
    }
    assert_eq!(&mut s, "REQUIRED INT32 foo;");
  }

  #[test]
  fn test_print_group_type() {
    let mut s = String::new();
    {
      let mut p = Printer::new(&mut s);
      let f1 = PrimitiveType::new(
        "f1", Repetition::REQUIRED, PhysicalType::INT32,
        LogicalType::INT_32, 0, 0, 0, Some(0));
      let f2 = PrimitiveType::new(
        "f2", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
        LogicalType::UTF8, 0, 0, 0, Some(1));
      let f3 = PrimitiveType::new(
        "f3", Repetition::REPEATED, PhysicalType::FIXED_LEN_BYTE_ARRAY,
        LogicalType::INTERVAL, 12, 0, 0, Some(2));
      let mut struct_fields: Vec<Box<Type>> = Vec::new();
      struct_fields.push(Box::new(f1.unwrap()));
      struct_fields.push(Box::new(f2.unwrap()));
      let foo = GroupType::new(
        "foo", Some(Repetition::OPTIONAL), LogicalType::NONE, struct_fields, Some(1)).unwrap();
      let mut fields: Vec<Box<Type>> = Vec::new();
      fields.push(Box::new(foo));
      fields.push(Box::new(f3.unwrap()));
      let mut message = GroupType::new(
        "schema", None, LogicalType::NONE, fields, Some(2)).unwrap();
      message.accept(&mut p);
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
