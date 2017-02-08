use std::fmt;

use basic::{Type as PhysicalType, LogicalType, Repetition};
use schema::types::{Type, PrimitiveType, GroupType, TypeVisitor};

const INDENT_WIDTH: i32 = 2;

pub struct Printer<'a> {
  output: &'a mut fmt::Write,
  indent: i32
}

#[allow(unused_must_use)]
impl <'a> Printer<'a> {
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
    write!(self.output, "{} {} {};", tp.repetition(), tp.physical_type(), tp.name());
  }

  fn visit_group_type(&mut self, tp: &mut GroupType) {
    self.print_indent();
    write!(self.output, "{} group {} ", tp.repetition(), tp.name());
    if tp.logical_type() != LogicalType::NONE {
      write!(self.output, "({}) ", tp.logical_type());
    }
    writeln!(self.output, "{{");
    self.indent += INDENT_WIDTH;
    for c in tp.fields() {
      c.accept(self);
      writeln!(self.output, "");
    }
    self.indent -= INDENT_WIDTH;
    write!(self.output, "}}");
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use schema::types::Type;

  #[test]
  fn test_print_primitive_type() {
    let mut s = String::new();
    {
      let mut p = Printer{ output: &mut s, indent: 0 };
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
      let mut p = Printer{ output: &mut s, indent: 0 };
      let mut fields: Vec<Box<Type>> = Vec::new();
      let f1 = PrimitiveType::new(
        "f1", Repetition::REQUIRED, PhysicalType::INT32,
        LogicalType::INT_32, 0, 0, 0, Some(0));
      let f2 = PrimitiveType::new(
        "f2", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
        LogicalType::UTF8, 0, 0, 0, Some(1));
      fields.push(Box::new(f1.unwrap()));
      fields.push(Box::new(f2.unwrap()));
      let mut foo = GroupType::new(
        "foo", Repetition::OPTIONAL, LogicalType::NONE, fields, Some(1)).unwrap();
      foo.accept(&mut p);
    }
    let expected =
"OPTIONAL group foo {
  REQUIRED INT32 f1;
  OPTIONAL BYTE_ARRAY f2;
}";
    assert_eq!(&mut s, expected);
  }

}
