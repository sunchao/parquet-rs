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
use std::rc::Rc;
use std::collections::HashMap;
use basic::{Type as PhysicalType, Repetition, LogicalType};
use errors::Result;
use errors::ParquetError;
use parquet_thrift::parquet::SchemaElement;

// ----------------------------------------------------------------------
// Parquet Type definitions

pub type TypePtr = Rc<Type>;
pub type SchemaDescPtr = Rc<SchemaDescriptor>;
pub type ColumnDescPtr = Rc<ColumnDescriptor>;

/// Representation of a Parquet type. Note that the top-level schema type
/// is represented using `GroupType` whose repetition is `None`.
#[derive(Debug, PartialEq)]
pub enum Type {
  PrimitiveType {
    basic_info: BasicTypeInfo, physical_type: PhysicalType,
    type_length: i32, scale: i32, precision: i32
  },
  GroupType {
    basic_info: BasicTypeInfo, fields: Vec<TypePtr>
  }
}

impl Type {
  // Create a new `PrimitiveType` instance from the input parameters
  // This also checks various illegal conditions and returns `Err` in case
  // that happens.
  pub fn new_primitive_type(
    name: &str, repetition: Repetition,
    physical_type: PhysicalType, logical_type: LogicalType, length: i32,
    precision: i32, scale: i32, id: Option<i32>) -> Result<Type> {

    let basic_info = BasicTypeInfo {
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
    logical_type: LogicalType, fields: Vec<TypePtr>, id: Option<i32>) -> Result<Type> {

    let basic_info = BasicTypeInfo {
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

  pub fn name(&self) -> &str {
    self.get_basic_info().name()
  }

  /// Get the fields from this group type.
  /// NOTE: this will panic if called on a non-group type.
  // TODO: should we return `&[&Type]` here?
  pub fn get_fields(&self) -> &[TypePtr] {
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
#[derive(Debug, PartialEq)]
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


// ----------------------------------------------------------------------
// Parquet descriptor definitions

/// Represents a path in a nested schema
#[derive(Clone, PartialEq, Debug)]
pub struct ColumnPath {
  parts: Vec<String>
}

impl ColumnPath {
  pub fn new(parts: Vec<String>) -> Self {
    ColumnPath { parts: parts }
  }

  pub fn string(&self) -> String {
    self.parts.join(".")
  }
}

impl fmt::Display for ColumnPath {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}", self.string())
  }
}

/// A descriptor for leaf-level primitive columns. This encapsulates
/// information such as definition and repetition levels and is used to
/// re-assemble nested data.
pub struct ColumnDescriptor {
  // The "leaf" primitive type of this column
  primitive_type: TypePtr,

  // The root type of this column. For instance,
  // if the column is "a.b.c.d", then the primitive type
  // is 'd' while the root_type is 'a'.
  root_type: TypePtr,

  // The maximum definition level for this column
  max_def_level: i16,

  // The maximum repetition level for this column
  max_rep_level: i16,

  // The path of this column. For instance, "a.b.c.d".
  path: ColumnPath
}

impl ColumnDescriptor {
  // Ctor is private outside of this module - should be created
  // via `SchemaDescriptor`.
  fn new(primitive_type: TypePtr, root_type: TypePtr,
         max_def_level: i16, max_rep_level: i16, path: ColumnPath) -> Self {
    Self { primitive_type, root_type, max_def_level, max_rep_level, path }
  }

  pub fn max_def_level(&self) -> i16 {
    self.max_def_level
  }

  pub fn max_rep_level(&self) -> i16 {
    self.max_rep_level
  }

  pub fn path(&self) -> &ColumnPath {
    &self.path
  }

  pub fn root_type(&self) -> &Type {
    self.root_type.as_ref()
  }

  pub fn name(&self) -> &str {
    self.primitive_type.name()
  }

  pub fn logical_type(&self) -> LogicalType {
    self.primitive_type.get_basic_info().logical_type()
  }

  pub fn physical_type(&self) -> PhysicalType {
    match self.primitive_type.as_ref() {
      &Type::PrimitiveType{ physical_type, .. } => physical_type,
      _ => panic!("Expected primitive type!")
    }
  }

  pub fn type_length(&self) -> i32 {
    match self.primitive_type.as_ref() {
      &Type::PrimitiveType{ type_length, .. } => type_length,
      _ => panic!("Expected primitive type!")
    }
  }

  pub fn type_precision(&self) -> i32 {
    match self.primitive_type.as_ref() {
      &Type::PrimitiveType{ precision, .. } => precision,
      _ => panic!("Expected primitive type!")
    }
  }

  pub fn type_scale(&self) -> i32 {
    match self.primitive_type.as_ref() {
      &Type::PrimitiveType{ scale, .. } => scale,
      _ => panic!("Expected primitive type!")
    }
  }

}

/// A schema descriptor. This encapsulates the top-level schemas for all
/// the columns, as well as all descriptors for all the primitive columns.
pub struct SchemaDescriptor {
  // The top-level schema (the "message" type).
  // This must be a `GroupType` where each field is a root column type in the schema.
  schema: TypePtr,

  // All the descriptors for primitive columns in this schema, constructed from
  // `schema` in DFS order.
  leaves: Vec<ColumnDescPtr>,

  // Mapping from a leaf column's index to the root column type that it
  // comes from. For instance: the leaf `a.b.c.d` would have a link back to `a`:
  // -- a  <------
  // -- -- b     |
  // -- -- -- c  |
  // -- -- -- -- d
  leaf_to_base: HashMap<usize, TypePtr>
}

impl SchemaDescriptor {
  pub fn new(tp: TypePtr) -> Self {
    assert!(tp.is_group(), "SchemaDescriptor should take a GroupType");
    let mut leaves = vec!();
    let mut leaf_to_base = HashMap::new();
    for f in tp.get_fields() {
      let mut path = vec!();
      build_tree(f.clone(), tp.clone(), f.clone(), 0, 0, &mut leaves, &mut leaf_to_base, &mut path);
    }
    Self { schema: tp, leaves: leaves, leaf_to_base: leaf_to_base }
  }

  pub fn column(&self, i: usize) -> ColumnDescPtr {
    assert!(i < self.leaves.len(),
            "Index out of bound: {} not in [0, {})", i, self.leaves.len());
    self.leaves[i].clone()
  }

  pub fn columns(&self) -> &[ColumnDescPtr] {
    &self.leaves
  }

  pub fn num_columns(&self) -> usize {
    self.leaves.len()
  }

  pub fn get_column_root(&self, i: usize) -> &Type {
    assert!(i < self.leaves.len(),
            "Index out of bound: {} not in [0, {})", i, self.leaves.len());
    let result = self.leaf_to_base.get(&i);
    assert!(result.is_some(), "Expected a value for index {} but found None", i);
    result.unwrap().as_ref()
  }

  pub fn root_schema(&self) -> &Type {
    self.schema.as_ref()
  }

  pub fn name(&self) -> &str {
    self.schema.name()
  }
}

fn build_tree(tp: TypePtr, root_tp: TypePtr, base_tp: TypePtr,
              mut max_rep_level: i16, mut max_def_level: i16,
              leaves: &mut Vec<ColumnDescPtr>,
              leaf_to_base: &mut HashMap<usize, TypePtr>,
              path_so_far: &mut Vec<String>) {
  assert!(tp.get_basic_info().has_repetition());

  path_so_far.push(String::from(tp.name()));
  match tp.get_basic_info().repetition() {
    Repetition::OPTIONAL => max_rep_level += 1,
    Repetition::REPEATED => {
      max_def_level += 1;
      max_rep_level += 1;
    },
    _ => { }
  }

  match tp.as_ref() {
    &Type::PrimitiveType{ .. } => {
      let mut path: Vec<String> = vec!();
      path.extend_from_slice(&path_so_far[..]);
      leaves.push(Rc::new(ColumnDescriptor::new(
        tp.clone(), root_tp, max_rep_level, max_def_level, ColumnPath::new(path))));
      leaf_to_base.insert(leaves.len() - 1, base_tp);
    },
    &Type::GroupType{ ref fields, .. } => {
      for f in fields {
        build_tree(f.clone(), root_tp.clone(), base_tp.clone(),
                   max_def_level, max_rep_level, leaves,
                   leaf_to_base, path_so_far);
        let idx = path_so_far.len() - 1;
        path_so_far.remove(idx);
      }
    }
  }
}

/// Conversion from Thrift equivalents

pub fn from_thrift(elements: &mut [SchemaElement]) -> Result<TypePtr> {
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
fn from_thrift_helper(elements: &mut [SchemaElement], index: usize) -> Result<(usize, TypePtr)> {
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
      Ok((index + 1, Rc::new(result)))
    },
    Some(n) => {
      let repetition = elements[index].repetition_type.map(|r| Repetition::from(r));
      let mut fields = vec!();
      let mut next_index = index + 1;
      for _ in 0..n {
        let child_result = from_thrift_helper(elements, next_index as usize)?;
        next_index = child_result.0;
        fields.push(child_result.1);
      }

      let group_tp = Type::new_group_type(
        &elements[index].name, repetition, logical_type, fields, field_id)?;
      Ok((next_index, Rc::new(group_tp)))
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
    let f1 = Type::new_primitive_type(
      "f1", Repetition::OPTIONAL, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, Some(0));
    assert!(f1.is_ok());
    let f2 = Type::new_primitive_type(
      "f2", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
      LogicalType::UTF8, 0, 0, 0, Some(1));
    assert!(f2.is_ok());

    let mut fields = vec!();
    fields.push(Rc::new(f1.unwrap()));
    fields.push(Rc::new(f2.unwrap()));

    let result = Type::new_group_type(
      "foo", Some(Repetition::REPEATED), LogicalType::NONE, fields, Some(1));
    assert!(result.is_ok());

    let tp = result.unwrap();
    let basic_info = tp.get_basic_info();
    assert_eq!(basic_info.repetition(), Repetition::REPEATED);
    assert_eq!(basic_info.logical_type(), LogicalType::NONE);
    assert_eq!(basic_info.id(), 1);
    assert_eq!(tp.get_fields().len(), 2);
    assert_eq!(tp.get_fields()[0].name(), "f1");
    assert_eq!(tp.get_fields()[1].name(), "f2");
  }

  #[test]
  fn test_column_descriptor() {
    let result = Type::new_primitive_type(
      "name", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
      LogicalType::UTF8, 0, 0, 0, None);
    assert!(result.is_ok());
    let tp = result.unwrap();

    let result = Type::new_group_type(
      "root", None, LogicalType::LIST, vec!(), None);
    assert!(result.is_ok());
    let root_tp = result.unwrap();
    let root_tp_rc = Rc::new(root_tp);

    let path = vec!(String::from("name"));
    let descr = ColumnDescriptor::new(Rc::new(tp), root_tp_rc.clone(), 4, 1, ColumnPath::new(path));

    assert_eq!(descr.path(), &ColumnPath::new(vec!(String::from("name"))));
    assert_eq!(descr.logical_type(), LogicalType::UTF8);
    assert_eq!(descr.physical_type(), PhysicalType::BYTE_ARRAY);
    assert_eq!(descr.max_def_level(), 4);
    assert_eq!(descr.max_rep_level(), 1);
    assert_eq!(descr.name(), "name");
    assert_eq!(descr.type_length(), 0);
    assert_eq!(descr.type_precision(), 0);
    assert_eq!(descr.type_scale(), 0);
    assert_eq!(descr.root_type(), root_tp_rc.as_ref());
  }

  #[test]
  fn test_schema_descriptor() {
    let _ = test_schema_descriptor_helper();
  }

  // A helper fn to avoid handling the results from type creation
  fn test_schema_descriptor_helper() -> Result<()> {
    let mut fields = vec!();

    let inta = Type::new_primitive_type(
      "a", Repetition::REQUIRED, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, None)?;
    fields.push(Rc::new(inta));
    fields.push(Rc::new(Type::new_primitive_type(
      "b", Repetition::OPTIONAL, PhysicalType::INT64,
      LogicalType::INT_64, 0, 0, 0, None)?));
    fields.push(Rc::new(Type::new_primitive_type(
      "c", Repetition::REPEATED, PhysicalType::BYTE_ARRAY,
      LogicalType::UTF8, 0, 0, 0, None)?));

    // 3-level list encoding
    let item1 = Type::new_primitive_type(
      "item1", Repetition::REQUIRED, PhysicalType::INT64,
      LogicalType::INT_64, 0, 0, 0, None)?;
    let item2 = Type::new_primitive_type(
      "item2", Repetition::OPTIONAL, PhysicalType::BOOLEAN,
      LogicalType::NONE, 0, 0, 0, None)?;
    let item3 = Type::new_primitive_type(
      "item3", Repetition::REPEATED, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, None)?;
    let list = Type::new_group_type(
      "records", Some(Repetition::REPEATED), LogicalType::LIST,
      vec!(Rc::new(item1), Rc::new(item2), Rc::new(item3)), None)?;
    let bag = Type::new_group_type(
      "bag", Some(Repetition::OPTIONAL), LogicalType::NONE,
      vec!(Rc::new(list)), None)?;
    fields.push(Rc::new(bag));

    let result = Type::new_group_type(
      "schema", Some(Repetition::REPEATED), LogicalType::NONE, fields, None);
    if result.is_err() {
      println!("ERROR: {:?}", result.as_ref().err().unwrap());
    }

    assert!(result.is_ok());
    let schema = Rc::new(result.unwrap());
    let descr = SchemaDescriptor::new(schema);

    let nleaves = 6;
    assert_eq!(descr.num_columns(), nleaves);

    //                             mdef mrep
    // required int32 a            0    0
    // optional int64 b            1    0
    // repeated byte_array c       1    1
    // optional group bag          1    0
    //   repeated group records    2    1
    //     required int64 item1    2    1
    //     optional boolean item2  3    1
    //     repeated int32 item3    3    2
    let ex_max_def_levels = vec!(0, 1, 1, 2, 3, 3);
    let ex_max_rep_levels = vec!(0, 0, 1, 1, 1, 2);

    for i in 0..nleaves {
      let col = descr.column(i);
      assert_eq!(col.max_def_level(), ex_max_def_levels[i], "{}", i);
      assert_eq!(col.max_rep_level(), ex_max_rep_levels[i], "{}", i);
    }

    assert_eq!(descr.column(0).path().string(), "a");
    assert_eq!(descr.column(1).path().string(), "b");
    assert_eq!(descr.column(2).path().string(), "c");
    assert_eq!(descr.column(3).path().string(), "bag.records.item1");
    assert_eq!(descr.column(4).path().string(), "bag.records.item2");
    assert_eq!(descr.column(5).path().string(), "bag.records.item3");

    assert_eq!(descr.get_column_root(0).name(), "a");
    assert_eq!(descr.get_column_root(3).name(), "bag");
    assert_eq!(descr.get_column_root(4).name(), "bag");
    assert_eq!(descr.get_column_root(5).name(), "bag");

    Ok(())
  }
}
