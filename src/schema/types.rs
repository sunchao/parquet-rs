use basic::{Type as PhysicalType, Repetition, LogicalType};
use errors::Result;
use errors::ParquetError;
use parquet_thrift::parquet::SchemaElement;

enum TypeKind {
  PRIMITIVE,
  GROUP
}

// TODO: how can we specify a return type for the methods?
// This seems tricky since Rust doesn't allow generic methods for trait objects.
pub trait TypeVisitor {
  fn visit_primitive_type(&mut self, tp: &mut PrimitiveType);
  fn visit_group_type(&mut self, tp: &mut GroupType);
}

/// A trait for a logical schema type. Structs who implement
/// this needs to implement the `get_basic_info()` function.
pub trait Type {
  fn is_primitive(&self) -> bool {
    match self.get_basic_info().kind {
      TypeKind::PRIMITIVE => true,
      _ => false
    }
  }

  fn is_group(&self) -> bool {
    match self.get_basic_info().kind {
      TypeKind::GROUP => true,
      _ => false
    }
  }

  fn name(&self) -> &str {
    &self.get_basic_info().name
  }

  fn repetition(&self) -> Repetition {
    self.get_basic_info().repetition
  }

  fn logical_type(&self) -> LogicalType {
    self.get_basic_info().logical_type
  }

  fn id(&self) -> Option<i32> {
    self.get_basic_info().id
  }

  /// Get the basic type information.
  fn get_basic_info(&self) -> &BasicTypeInfo;

  /// Accept a `TypeVisitor` to visit the concrete type of this trait.
  fn accept(&mut self, visitor: &mut TypeVisitor);
}

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
pub struct BasicTypeInfo {
  kind: TypeKind,
  name: String,
  repetition: Repetition,
  logical_type: LogicalType,
  id: Option<i32>
}

/// Metadata for a decimal type (scale, precision).
struct DecimalMetadata {
  scale: i32,
  precision: i32
}

/// Representation of primitive types
// TODO: add equality
pub struct PrimitiveType {
  basic_info: BasicTypeInfo,
  physical_type: PhysicalType,
  type_length: i32,
  decimal_metadata: Option<DecimalMetadata>
}

impl PrimitiveType {
  pub fn new(name: &str, repetition: Repetition, physical_type: PhysicalType,
             logical_type: LogicalType, length: i32,
             precision: i32, scale: i32, id: Option<i32>) -> Result<Self> {
    let mut decimal_metadata = None;
    let basic_info = BasicTypeInfo{
      kind: TypeKind::PRIMITIVE, name: String::from(name), repetition: repetition,
      logical_type: logical_type, id: id};

    match logical_type {
      LogicalType::NONE => {
        decimal_metadata = Some(DecimalMetadata{precision, scale});
      },
      LogicalType::UTF8 | LogicalType::BSON | LogicalType::JSON => {
        if physical_type != PhysicalType::BYTE_ARRAY {
          return Err(parse_err!("{} can only annotate BYTE_ARRAY fields", logical_type))
        }
      },
      LogicalType::DECIMAL => {
        match physical_type {
          PhysicalType::INT32 | PhysicalType::INT64 | PhysicalType::BYTE_ARRAY |
          PhysicalType::FIXED_LEN_BYTE_ARRAY => (),
          _ => {
            return Err(parse_err!("DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED"))
          }
        };
        if precision < 0 {
          return Err(parse_err!("Invalid DECIMAL precision: {}", precision))
        }
        if scale < 0 {
          return Err(parse_err!("Invalid DECIMAL scale: {}", scale))
        }
        if scale > precision {
          return Err(parse_err!(
            "Invalid DECIMAL: scale ({}) cannot be greater than precision ({})",
            scale, precision))
        }
        decimal_metadata = Some(DecimalMetadata{precision, scale})
      }
      LogicalType::DATE | LogicalType::TIME_MILLIS | LogicalType::UINT_8 |
      LogicalType::UINT_16 | LogicalType::UINT_32 |
      LogicalType::INT_8 | LogicalType::INT_16 | LogicalType::INT_32 => {
        if physical_type != PhysicalType::INT32 {
          return Err(parse_err!("{} can only annotate INT32", logical_type))
        }
      }
      LogicalType::TIME_MICROS | LogicalType::TIMESTAMP_MILLIS |
      LogicalType::TIMESTAMP_MICROS | LogicalType::UINT_64 | LogicalType::INT_64 => {
        if physical_type != PhysicalType::INT64 {
          return Err(parse_err!("{} can only annotate INT64", logical_type))
        }
      }
      LogicalType::INTERVAL => {
        if physical_type != PhysicalType::FIXED_LEN_BYTE_ARRAY || length != 12 {
          return Err(parse_err!("INTERVAL can only annotate FIXED(12)"))
        }
      }
      LogicalType::ENUM => {
        if physical_type != PhysicalType::BYTE_ARRAY {
          return Err(parse_err!("ENUM can only annotate BYTE_ARRAY fields"))
        }
      }
      _ => {
        return Err(parse_err!("{} cannot be applied to a primitive type", logical_type))
      }
    };
    if physical_type == PhysicalType::FIXED_LEN_BYTE_ARRAY && length < 0 {
      return Err(parse_err!("Invalid FIXED_LEN_BYTE_ARRAY length: {}", length))
    }
    Ok(PrimitiveType{
      basic_info: basic_info,
      physical_type: physical_type,
      type_length: length,
      decimal_metadata: decimal_metadata
    })
  }

  pub fn physical_type(&self) -> PhysicalType {
    self.physical_type
  }

  fn decimal_metadata(&self) -> &Option<DecimalMetadata> {
    &self.decimal_metadata
  }
}

impl Type for PrimitiveType {
  fn get_basic_info(&self) -> &BasicTypeInfo {
    &self.basic_info
  }

  fn accept(&mut self, visitor: &mut TypeVisitor) {
    visitor.visit_primitive_type(self)
  }
}

/// Representation of group types
// TODO: add equality
pub struct GroupType {
  basic_info: BasicTypeInfo,
  fields: Vec<Box<Type>>
}

impl GroupType {
  pub fn new(name: &str, repetition: Repetition,
             logical_type: LogicalType,
             fields: Vec<Box<Type>>, id: Option<i32>) -> Result<Self> {
    let basic_info = BasicTypeInfo{
      kind: TypeKind::GROUP, name: String::from(name), repetition: repetition,
      logical_type: logical_type, id: id};
    Ok(GroupType{
      basic_info: basic_info,
      fields: fields
    })
  }

  pub fn fields(&mut self) -> &mut Vec<Box<Type>> {
    &mut self.fields
  }

  pub fn num_fields(&self) -> usize {
    self.fields.len()
  }

  pub fn field(&mut self, index: usize) -> &mut Box<Type> {
    &mut self.fields[index]
  }
}

impl Type for GroupType {
  fn get_basic_info(&self) -> &BasicTypeInfo {
    &self.basic_info
  }

  fn accept(&mut self, visitor: &mut TypeVisitor) {
    visitor.visit_group_type(self)
  }
}


/// Conversion from Thrift equivalents

pub fn from_thrift(elements: &mut [SchemaElement]) -> Result<Vec<Box<Type>>> {
  let mut index = 1;
  let mut result = Vec::new();
  // TODO: do we need to process the root element?
  while index < elements.len() {
    let t = from_thrift_helper(elements, index)?;
    index = t.0;
    result.push(t.1);
  }
  Ok(result)
}

/// Construct a new Type from the `elements`, starting at index `index`.
/// The first result is the starting index for the next Type after this one.
/// If it is equal to `elements.len()`, then this Type is the last one.
/// The second result is the result Type.
fn from_thrift_helper(elements: &mut [SchemaElement], index: usize) -> Result<(usize, Box<Type>)> {
  if index < 0 {
    return Err(schema_err!("Illegal index ({}). Expect to be non-negative.", index))
  }
  if index > elements.len() {
    return Err(schema_err!("Index out of bound, index = {}, len = {}", index, elements.len()))
  }
  if elements[index].repetition_type.is_none() {
    return Err(schema_err!("Repetition is undefined"));
  }
  let repetition = Repetition::from(elements[index].repetition_type.unwrap());
  let logical_type = LogicalType::from(elements[index].converted_type);
  let field_id = elements[index].field_id;
  match elements[index].num_children {
    None => {
      // primitive type
      let physical_type = PhysicalType::from(elements[index].type_.unwrap());
      let length = elements[index].type_length.unwrap_or(-1);
      let scale = elements[index].scale.unwrap_or(-1);
      let precision = elements[index].precision.unwrap_or(-1);
      let name = &elements[index].name;
      let result = PrimitiveType::new(
        name, repetition, physical_type, logical_type, length, precision, scale, field_id)?;
      Ok((index + 1, Box::new(result)))
    },
    Some(n) => {
      let mut fields: Vec<Box<Type>> = Vec::new();
      let mut next_index = index + 1;
      for _ in 0..n {
        let child_result = from_thrift_helper(elements, next_index as usize)?;
        next_index = child_result.0;
        fields.push(child_result.1);
      }
      let name = &elements[index].name;
      let result = GroupType::new(
        name, repetition, logical_type, fields, field_id)?;
      Ok((next_index, Box::new(result)))
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::error::Error;

  #[test]
  fn test_primitive_type() {
    let mut result = PrimitiveType::new(
      "foo", Repetition::OPTIONAL, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, Some(0));
    assert!(result.is_ok());

    if let Ok(tp) = result {
      assert!(tp.is_primitive());
      assert!(!tp.is_group());
      assert_eq!(tp.repetition(), Repetition::OPTIONAL);
      assert_eq!(tp.logical_type(), LogicalType::INT_32);
      assert_eq!(tp.physical_type(), PhysicalType::INT32);
      assert_eq!(tp.id(), Some(0));
      assert!(tp.decimal_metadata().is_none());
    }

    // Test illegal inputs
    result = PrimitiveType::new(
      "foo", Repetition::REPEATED, PhysicalType::INT64,
      LogicalType::BSON, 0, 0, 0, None);
    assert!(result.is_err());
    if let Err(e) = result {
      assert_eq!(e.description(), "BSON can only annotate BYTE_ARRAY fields");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::INT96,
      LogicalType::DECIMAL, 0, -1, -1, None);
    assert!(result.is_err());
    if let Err(e) = result {
      assert_eq!(e.description(), "DECIMAL can only annotate INT32, INT64, BYTE_ARRAY and FIXED");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::DECIMAL, 0, -1, -1, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "Invalid DECIMAL precision: -1");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::DECIMAL, 0, 0, -1, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "Invalid DECIMAL scale: -1");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::DECIMAL, 0, 1, 2, None);
    if let Err(e) = result {
      assert_eq!(
        e.description(),
        "Invalid DECIMAL: scale (2) cannot be greater than precision (1)");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::INT64,
      LogicalType::UINT_8, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "UINT_8 can only annotate INT32");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::INT32,
      LogicalType::TIME_MICROS, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "TIME_MICROS can only annotate INT64");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::BYTE_ARRAY,
      LogicalType::INTERVAL, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "INTERVAL can only annotate FIXED(12)");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::FIXED_LEN_BYTE_ARRAY,
      LogicalType::INTERVAL, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "INTERVAL can only annotate FIXED(12)");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::INT32,
      LogicalType::ENUM, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "ENUM can only annotate BYTE_ARRAY fields");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::INT32,
      LogicalType::MAP, 0, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "MAP cannot be applied to a primitive type");
    }

    result = PrimitiveType::new(
      "foo", Repetition::REQUIRED, PhysicalType::FIXED_LEN_BYTE_ARRAY,
      LogicalType::DECIMAL, -1, 0, 0, None);
    if let Err(e) = result {
      assert_eq!(e.description(), "Invalid FIXED_LEN_BYTE_ARRAY length: -1");
    }
  }

  #[test]
  fn test_group_type() {
    // TODO: why Rust require an explicit type annotation here?
    let mut fields: Vec<Box<Type>> = Vec::new();
    let f1 = PrimitiveType::new(
      "f1", Repetition::OPTIONAL, PhysicalType::INT32,
      LogicalType::INT_32, 0, 0, 0, Some(0));
    let f2 = PrimitiveType::new(
      "f2", Repetition::OPTIONAL, PhysicalType::BYTE_ARRAY,
      LogicalType::UTF8, 0, 0, 0, Some(1));
    assert!(f1.is_ok());
    assert!(f2.is_ok());
    fields.push(Box::new(f1.unwrap()));
    fields.push(Box::new(f2.unwrap()));
    let result = GroupType::new(
      "foo", Repetition::REPEATED, LogicalType::NONE, fields, Some(1));
    assert!(result.is_ok());
    if let Ok(tp) = result {
      assert_eq!(tp.repetition(), Repetition::REPEATED);
      assert_eq!(tp.logical_type(), LogicalType::NONE);
      assert_eq!(tp.id(), Some(1));
      assert_eq!(tp.num_fields(), 2);
      // TODO: test fields equality once that is implemented.
    }
  }
}
