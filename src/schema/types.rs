use basic::{Type as PhysicalType, Repetition, LogicalType};

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
  name: String,
  repetition: Repetition,
  logical_type: LogicalType,
  id: i32
}

/// Metadata for a decimal type (scale, precision).
struct DecimalMetadata {
  isset: bool,
  scale: i32,
  precision: i32
}

struct PrimitiveType {
  physical_type: PhysicalType,
  type_length: i32,
  decimal_metadata: DecimalMetadata
}
