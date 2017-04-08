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

use basic::{Type as PhysicalType, Repetition, LogicalType};
use errors::Result;
use errors::ParquetError;
use parquet_thrift::parquet::SchemaElement;

/// Representation of a Parquet type. Note that the top-level schema type
/// is represented using `GroupType` whose repetition is `None`.
pub enum Type {
  PrimitiveType {
    basic_info: BasicTypeInfo, physical_type: PhysicalType,
    type_length: i32, scale: i32, precision: i32
  },
  GroupType {
    basic_info: BasicTypeInfo, fields: Vec<Type>
  }
}

impl Type {
  // Create a new `PrimitiveType` instance from the input parameters
  // This also checks various illegal conditions and returns `Err` in case
  // that happens.
  pub fn new_primitive_type(
    name: &str, repetition: Repetition, physical_type: PhysicalType,
    logical_type: LogicalType, length: i32,
    precision: i32, scale: i32, id: Option<i32>) -> Result<Type> {

    let basic_info = BasicTypeInfo{
      name: String::from(name), repetition: Some(repetition),
      logical_type: logical_type, id: id };

    match logical_type {
      LogicalType::NONE => {
      },
      LogicalType::UTF8 | LogicalType::BSON | LogicalType::JSON => {
        if physical_type != PhysicalType::BYTE_ARRAY {
          return general_err!("{} can only annotate BYTE_ARRAY fields", logical_type)
        }
      },
      LogicalType::DECIMAL => {
        match physical_type {
          PhysicalType::INT32 | PhysicalType::INT64 | PhysicalType::BYTE_ARRAY |
          PhysicalType::FIXED_LEN_BYTE_ARRAY => (),
          _ => {
            return general_err!("DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED")
          }
        };
        if precision < 0 {
          return general_err!("Invalid DECIMAL precision: {}", precision)
        }
        if scale < 0 {
          return general_err!("Invalid DECIMAL scale: {}", scale)
        }
        if scale > precision {
          return general_err!(
            "Invalid DECIMAL: scale ({}) cannot be greater than precision ({})",
            scale, precision)
        }
      }
      LogicalType::DATE | LogicalType::TIME_MILLIS | LogicalType::UINT_8 |
      LogicalType::UINT_16 | LogicalType::UINT_32 |
      LogicalType::INT_8 | LogicalType::INT_16 | LogicalType::INT_32 => {
        if physical_type != PhysicalType::INT32 {
          return general_err!("{} can only annotate INT32", logical_type)
        }
      }
      LogicalType::TIME_MICROS | LogicalType::TIMESTAMP_MILLIS |
      LogicalType::TIMESTAMP_MICROS | LogicalType::UINT_64 | LogicalType::INT_64 => {
        if physical_type != PhysicalType::INT64 {
          return general_err!("{} can only annotate INT64", logical_type)
        }
      }
      LogicalType::INTERVAL => {
        if physical_type != PhysicalType::FIXED_LEN_BYTE_ARRAY || length != 12 {
          return general_err!("INTERVAL can only annotate FIXED(12)")
        }
      }
      LogicalType::ENUM => {
        if physical_type != PhysicalType::BYTE_ARRAY {
          return general_err!("ENUM can only annotate BYTE_ARRAY fields")
        }
      }
      _ => {
        return general_err!("{} cannot be applied to a primitive type", logical_type)
      }
    };
    if physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY && length < 0 {
      return general_err!("Invalid FIXED_LEN_BYTE_ARRAY length: {}", length)
    }
    Ok(Type::PrimitiveType{
      basic_info: basic_info,
      physical_type: physical_type,
      type_length: length,
      scale: scale,
      precision: precision
    })
  }

  // Create a new `GroupType` instance from the input parameters.
  pub fn new_group_type(
    name: &str, repetition: Option<Repetition>,
    logical_type: LogicalType, fields: Vec<Type>, id: Option<i32>) -> Result<Type> {

    let basic_info = BasicTypeInfo{
      name: String::from(name), repetition: repetition,
      logical_type: logical_type, id: id };
    Ok(Type::GroupType{ basic_info: basic_info, fields: fields })
  }

  pub fn get_basic_info(&self) -> &BasicTypeInfo {
    match *self {
      Type::PrimitiveType { ref basic_info, .. } => &basic_info,
      Type::GroupType { ref basic_info, .. } => &basic_info
    }
  }

  /// Get the fields from this group type.
  /// NOTE: this will panic if called on a non-group type.
  pub fn get_fields(&self) -> &[Type] {
    match *self {
      Type::GroupType{ ref fields, .. } => &fields[..],
      _ => panic!("Cannot call get_fields() on a non-group type")
    }
  }

  fn is_primitive(&self) -> bool {
    match *self {
      Type::PrimitiveType{ .. } => true,
      _ => false
    }
  }

  fn is_group(&self) -> bool {
    match *self {
      Type::GroupType{ .. } => true,
      _ => false
    }
  }

  /// Whether this is the top-level schema type (message type).
  fn is_schema(&self) -> bool {
    match *self {
      Type::GroupType{ ref basic_info, .. } => !basic_info.has_repetition(),
      _ => false
    }
  }
}

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
pub struct BasicTypeInfo {
  name: String,
  repetition: Option<Repetition>,
  logical_type: LogicalType,
  id: Option<i32>,
}

impl BasicTypeInfo {
  pub fn name(&self) -> &str {
    &self.name
  }

  pub fn has_repetition(&self) -> bool {
    self.repetition.is_some()
  }

  pub fn repetition(&self) -> Repetition {
    assert!(self.repetition.is_some());
    self.repetition.unwrap()
  }

  pub fn logical_type(&self) -> LogicalType {
    self.logical_type
  }

  pub fn has_id(&self) -> bool {
    self.id.is_some()
  }

  pub fn id(&self) -> i32 {
    assert!(self.id.is_some());
    self.id.unwrap()
  }
}

/// A descriptor for leaf-level primitive columns. This encapsulates
/// information such as definition and repetition levels and is used to
/// re-assemble nested data.
pub struct ColumnDescriptor<'a> {
  // The "leaf" primitive type of this column
  primitive_type: &'a Type,

  // The root type of this column. For instance,
  // if the column is "a.b.c.d", then the primitive type
  // is 'd' while the root_type is 'a'.
  root_type: &'a Type,

  // The maximum definition level for this column
  max_def_level: i16,

  // The maximum repetition level for this column
  max_rep_level: i16
}

impl<'a> ColumnDescriptor<'a> {
  pub fn new(primitive_type: &'a Type, root_type: &'a Type,
             max_def_level: i16, max_rep_level: i16) -> Self {
    Self { primitive_type, root_type, max_def_level, max_rep_level }
  }

  pub fn max_def_level(&self) -> i16 {
    self.max_def_level
  }

  pub fn max_rep_level(&self) -> i16 {
    self.max_rep_level
  }
}

/// A schema descriptor. This encapsulates the top-level schemas for all
/// the columns, as well as all descriptors for all the primitive columns.
pub struct SchemaDescriptor<'a> {
  // The top-level schema
  schema: &'a Type,

  // All the descriptors for primitive columns in this schema
  leaves: Vec<ColumnDescriptor<'a>>
}

impl<'a> SchemaDescriptor<'a> {
  pub fn new(tp: &'a Type) -> Self {
    let mut leaves = vec!();
    build_tree(tp, tp, 0, 0, &mut leaves);
    Self { schema: tp, leaves: leaves }
  }
}

fn build_tree<'a>(tp: &'a Type, root_tp: &'a Type,
                  mut max_rep_level: i16, mut max_def_level: i16,
                  results: &mut Vec<ColumnDescriptor<'a>>) {
  assert!(tp.get_basic_info().has_repetition());
  match tp.get_basic_info().repetition() {
    Repetition::OPTIONAL => max_rep_level += 1,
    Repetition::REPEATED => {
      max_def_level += 1;
      max_rep_level += 1;
    },
    _ => { }
  }

  match tp {
    ptp @ &Type::PrimitiveType{ .. } => {
      results.push(ColumnDescriptor::new(ptp, root_tp, max_rep_level, max_def_level))
    },
    &Type::GroupType{ ref fields, .. } => {
      for f in fields {
        build_tree(&f, root_tp, max_def_level, max_rep_level, results);
      }
    }
  }
}

/// Conversion from Thrift equivalents

pub fn from_thrift(elements: &mut [SchemaElement]) -> Result<Type> {
  let mut index = 0;
  let mut schema_nodes = Vec::new();
  while index < elements.len() {
    let t = from_thrift_helper(elements, index)?;
    index = t.0;
    schema_nodes.push(t.1);
  }
  if schema_nodes.len() != 1 {
    return general_err!("Expected exactly one root node, but found {}", schema_nodes.len())
  }

  Ok(schema_nodes.remove(0))
}

/// Construct a new Type from the `elements`, starting at index `index`.
/// The first result is the starting index for the next Type after this one.
/// If it is equal to `elements.len()`, then this Type is the last one.
/// The second result is the result Type.
fn from_thrift_helper(elements: &mut [SchemaElement], index: usize) -> Result<(usize, Type)> {
  if index > elements.len() {
    return general_err!("Index out of bound, index = {}, len = {}", index, elements.len())
  }
  let logical_type = LogicalType::from(elements[index].converted_type);
  let field_id = elements[index].field_id;
  match elements[index].num_children {
    None => {
      // primitive type
      if elements[index].repetition_type.is_none() {
        return general_err!("Repetition level must be defined for a primitive type");
      }
      let repetition = Repetition::from(elements[index].repetition_type.unwrap());
      let physical_type = PhysicalType::from(elements[index].type_.unwrap());
      let length = elements[index].type_length.unwrap_or(-1);
      let scale = elements[index].scale.unwrap_or(-1);
      let precision = elements[index].precision.unwrap_or(-1);
      let name = &elements[index].name;
      let result = Type::new_primitive_type(
        name, repetition, physical_type, logical_type, length, precision, scale, field_id)?;
      Ok((index + 1, result))
    },
    Some(n) => {
      let repetition = elements[index].repetition_type.map(|r| Repetition::from(r));
      let mut fields = Vec::new();
      let mut next_index = index + 1;
      for _ in 0..n {
        let child_result = from_thrift_helper(elements, next_index as usize)?;
        next_index = child_result.0;
        fields.push(child_result.1);
      }
      let name = &elements[index].name;
      let result = Type::new_group_type(
        name, repetition, logical_type, fields, field_id)?;
      Ok((next_index, result))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::error::Error;

  #[test]
  fn test_primitive_type() {
    let mut result = Type::new_primitive_type(
      "foo", Repetition::OPTIONAL, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, Some(0));
    assert!(result.is_ok());

    if let Ok(tp) = result {
      assert!(tp.is_primitive());
      assert!(!tp.is_group());
      let basic_info = tp.get_basic_info();
      assert_eq!(basic_info.repetition(), Repetition::OPTIONAL);
      assert_eq!(basic_info.logical_type(), LogicalType::INT_32);
      assert_eq!(basic_info.id(), 0);
      match tp {
        Type::PrimitiveType{ physical_type, .. } => {
          assert_eq!(physical_type, PhysicalType::INT32);
        },
        _ => assert!(false)
      }
    }

    // Test illegal inputs
    result = Type::new_primitive_type(
      "foo", Repetition::REPEATED, PhysicalType::INT64,
      LogicalType::BSON, 0, 0, 0, None);
    assert!(result.is_err());
    if let Err(e) = result {
      assert_eq!(e.description(), "BSON can only annotate BYTE_ARRAY fields");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::INT96,
      LogicalType::DECIMAL, 0, -1, -1, None);
    assert!(result.is_err());
    if let Err(e) = result {
      assert_eq!(e.description(), "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::DECIMAL, 0, -1, -1, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "Invalid DECIMAL precision: -1");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::DECIMAL, 0, 0, -1, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "Invalid DECIMAL scale: -1");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::DECIMAL, 0, 1, 2, None);
    if let Err(e) = result {
      assert_eq!(
        e.description(),
        "Invalid DECIMAL: scale (2) cannot be greater than precision (1)");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::INT64,
      LogicalType::UINT_8, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "UINT_8 can only annotate INT32");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::INT32,
      LogicalType::TIME_MICROS, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "TIME_MICROS can only annotate INT64");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::INTERVAL, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "INTERVAL can only annotate FIXED(12)");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::FIXED_LEN_BYTE_ARRAY,
      LogicalType::INTERVAL, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "INTERVAL can only annotate FIXED(12)");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::INT32,
      LogicalType::ENUM, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "ENUM can only annotate BYTE_ARRAY fields");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::INT32,
      LogicalType::MAP, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "MAP cannot be applied to a primitive type");
    }

    result = Type::new_primitive_type(
      "foo", Repetition::REQUIRED, PhysicalType::FIXED_LEN_BYTE_ARRAY,
      LogicalType::DECIMAL, -1, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "Invalid FIXED_LEN_BYTE_ARRAY length: -1");
    }
  }

  #[test]
  fn test_group_type() {
    let mut fields = Vec::new();
    let f1 = Type::new_primitive_type(
      "f1", Repetition::OPTIONAL, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, Some(0));
    let f2 = Type::new_primitive_type(
      "f2", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
      LogicalType::UTF8, 0, 0, 0, Some(1));
    assert!(f1.is_ok());
    assert!(f2.is_ok());
    fields.push(f1.unwrap());
    fields.push(f2.unwrap());
    let result = Type::new_group_type(
      "foo", Some(Repetition::REPEATED), LogicalType::NONE, fields, Some(1));
    assert!(result.is_ok());
    if let Ok(tp) = result {
      let basic_info = tp.get_basic_info();
      assert_eq!(basic_info.repetition(), Repetition::REPEATED);
      assert_eq!(basic_info.logical_type(), LogicalType::NONE);
      assert_eq!(basic_info.id(), 1);
      assert_eq!(tp.get_fields().len(), 2);
      // TODO: test fields equality once that is implemented.
    }
  }
}
