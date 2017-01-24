use basic::{Type as PhysicalType, Repetition, LogicalType};
use errors::Result;
use errors::ParquetError;

enum TypeKind {
  PRIMITIVE,
  GROUP
}

/// A trait for a logical schema type. Structs who implement
/// this needs to implement the `get_basic_info()` function.
trait Type {
  fn is_primitive(&self) -> bool {
    return match self.get_basic_info().kind {
      TypeKind::PRIMITIVE => true,
      _ => false
    }
  }

  fn is_group(&self) -> bool {
    return match self.get_basic_info().kind {
      TypeKind::GROUP => true,
      _ => false
    }
  }

  fn name(&self) -> &str {
    return &self.get_basic_info().name;
  }

  fn repetition(&self) -> Repetition {
    return self.get_basic_info().repetition;
  }

  fn logical_type(&self) -> LogicalType {
    return self.get_basic_info().logical_type;
  }

  fn id(&self) -> i32 {
    return self.get_basic_info().id;
  }

  fn get_basic_info(&self) -> &BasicTypeInfo;
}

/// Basic type info. This contains information such as the name of the type,
/// the repetition level, the logical type and the kind of the type (group, primitive).
struct BasicTypeInfo {
  kind: TypeKind,
  name: &'static str,
  repetition: Repetition,
  logical_type: LogicalType,
  id: i32
}

/// Metadata for a decimal type (scale, precision).
struct DecimalMetadata {
  scale: i32,
  precision: i32
}

/// Representation of primitive types
struct PrimitiveType {
  basic_info: BasicTypeInfo,
  physical_type: PhysicalType,
  type_length: i32,
  decimal_metadata: Option<DecimalMetadata>
}

impl PrimitiveType {
  pub fn new(name: &'static str, repetition: Repetition, physical_type: PhysicalType,
             logical_type: LogicalType, length: i32,
             precision: i32, scale: i32, id: i32) -> Result<Self> {
    let mut decimal_metadata = None;
    let basic_info = BasicTypeInfo{
      kind: TypeKind::PRIMITIVE, name: name, repetition: repetition,
      logical_type: logical_type, id: id};

    // TODO: more validity checks
    match logical_type {
      LogicalType::NONE => {
        decimal_metadata = Some(DecimalMetadata{precision, scale});
      },
      LogicalType::UTF8 | LogicalType::BSON | LogicalType::JSON => {
        if physical_type != PhysicalType::BYTE_ARRAY {
          return Err(ParquetError::Schema(String::from("can only annotate BYTE_ARRAY fields")))
        }
      },
      LogicalType::DECIMAL => {
        if precision < 0 {
          // TODO: how can we make ParquetError::Schema to accept `&'static str`?
          return Err(ParquetError::Schema(
            format!("Precision must be non-negative, but found {}", precision)))
        }
        if scale < 0 {
          return Err(ParquetError::Schema(
            format!("Scale must be non-negative, but found {}", scale)))
        }
        decimal_metadata = Some(DecimalMetadata{precision, scale})
      }
      _ => {
        return Err(ParquetError::Schema(String::from("can only annotate BYTE_ARRAY fields")))
      }
    };
    Ok(PrimitiveType{
      basic_info: basic_info,
      physical_type: physical_type,
      type_length: length,
      decimal_metadata: decimal_metadata
    })
  }
}

impl Type for PrimitiveType {
  fn get_basic_info(&self) -> &BasicTypeInfo {
    return &self.basic_info;
  }
}

/// Representation of group types
struct GroupType {
  basic_info: BasicTypeInfo,
  fields: Vec<Box<Type>>
}

impl GroupType {
  pub fn new(name: &'static str, repetition: Repetition, physical_type: PhysicalType,
             logical_type: LogicalType, length: i32,
             fields: Vec<Box<Type>>, id: i32) -> Result<Self> {
    let basic_info = BasicTypeInfo{
      kind: TypeKind::PRIMITIVE, name: name, repetition: repetition,
      logical_type: logical_type, id: id};
    Ok(GroupType{
      basic_info: basic_info,
      fields: fields
    })
  }
}

impl Type for GroupType {
  fn get_basic_info(&self) -> &BasicTypeInfo {
    return &self.basic_info;
  }
}
