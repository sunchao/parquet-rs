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

use std::io::{BufWriter, Write};
use std::marker::PhantomData;
use basic::*;
use errors::{Result};
use schema::types::ColumnDescPtr;
use util::memory::{BytePtr, ByteBuffer};
use util::bit_util::{BitWriter, convert_to_bytes};

pub trait Encoder<T: DataType> {
  /// Encode `num_values` of values from `src`.
  fn put(&mut self, src: &[T::T], num_values: usize) -> Result<()>;

  /// Return the encoding type of this encoder.
  fn encoding(&self) -> Encoding;

  /// Consume the underlying byte buffer that's being processed
  /// by this encoder, and return it.
  fn consume_buffer(&mut self) -> BytePtr;
}


pub struct PlainEncoder<T: DataType> {
  out: BufWriter<ByteBuffer>,
  bit_writer: BitWriter,
  descr: ColumnDescPtr,
  _phantom: PhantomData<T>
}

impl<T: DataType> PlainEncoder<T> {
  pub fn new(out: BufWriter<ByteBuffer>, descr: ColumnDescPtr) -> Self {
    Self { out: out, bit_writer: BitWriter::new(256), descr: descr, _phantom: PhantomData }
  }
}

impl<T: DataType> Encoder<T> for PlainEncoder<T> {
  default fn put(&mut self, src: &[T::T], num_values: usize) -> Result<()> {
    assert!(src.len() >= num_values);
    for i in 0..num_values {
      let p: *const T::T = &src[i];
      let p: *const u8 = p as *const u8;
      let s: &[u8] = unsafe {
        ::std::slice::from_raw_parts(p, ::std::mem::size_of::<T::T>())
      };
      self.out.write(s)?;
    }
    self.out.flush()?;
    Ok(())
  }

  fn encoding(&self) -> Encoding {
    Encoding::PLAIN
  }

  fn consume_buffer(&mut self) -> BytePtr {
    self.out.get_mut().consume()
  }
}

impl Encoder<BoolType> for PlainEncoder<BoolType> {
  fn put(&mut self, src: &[bool], num_values: usize) -> Result<()> {
    assert!(src.len() >= num_values);
    for i in 0..num_values {
      self.bit_writer.put_value(src[i] as u64, 1);
    }

    // TODO: maybe we should write directly to `out`?
    let buf_ptr = self.bit_writer.consume();
    self.out.write(buf_ptr.slice())?;
    self.out.flush()?;
    Ok(())
  }
}

impl Encoder<Int96Type> for PlainEncoder<Int96Type> {
  fn put(&mut self, src: &[Int96], num_values: usize) -> Result<()> {
    assert!(src.len() >= num_values);
    for i in 0..num_values {
      for j in src[i].data() {
        self.out.write(&convert_to_bytes::<u32>(&j, ::std::mem::size_of::<u32>()))?;
      }
    }
    self.out.flush()?;
    Ok(())
  }
}

impl Encoder<ByteArrayType> for PlainEncoder<ByteArrayType> {
  fn put(&mut self, src: &[ByteArray], num_values: usize) -> Result<()> {
    assert!(src.len() >= num_values);
    for i in 0..num_values {
      let len_bytes = convert_to_bytes::<u32>(&(src[i].len().to_le() as u32), ::std::mem::size_of::<u32>());
      self.out.write(&len_bytes)?;
      self.out.write(src[i].data())?;
    }
    self.out.flush()?;
    Ok(())
  }
}

impl Encoder<FixedLenByteArrayType> for PlainEncoder<FixedLenByteArrayType> {
  fn put(&mut self, src: &[ByteArray], num_values: usize) -> Result<()> {
    assert!(src.len() >= num_values);
    for i in 0..num_values {
      self.out.write(src[i].data())?;
    }
    self.out.flush()?;
    Ok(())
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use super::super::decoding::*;
  use std::rc::Rc;
  use schema::types::{Type as SchemaType, ColumnDescriptor, ColumnPath};
  use util::test_common::RandGen;

  const TEST_SET_SIZE: usize = 32;

  #[test]
  fn test_bool() {
    BoolType::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  #[test]
  fn test_i32() {
    Int32Type::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  #[test]
  fn test_i64() {
    Int64Type::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  #[test]
  fn test_i96() {
    Int96Type::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  #[test]
  fn test_float() {
    FloatType::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  #[test]
  fn test_double() {
    DoubleType::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  #[test]
  fn test_byte_array() {
    ByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  #[test]
  fn test_fixed_lenbyte_array() {
    FixedLenByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE);
  }

  trait EncodingTester<T: DataType> {
    fn test(enc: Encoding, total: usize) {
      let result = Self::test_internal(enc, total);
      assert!(result.is_ok(), "Expected result to be OK but got err:\n {}", result.unwrap_err());
    }

    fn test_internal(enc: Encoding, total: usize) -> Result<()>;
  }

  impl<T: DataType> EncodingTester<T> for T where T: 'static {
    default fn test_internal(enc: Encoding, total: usize) -> Result<()> {
      let mut encoder = create_test_encoder::<T>(-1, enc);
      let values = <T as RandGen<T>>::gen_vec(-1, total);
      encoder.put(&values[..], total)?;

      let data = encoder.consume_buffer();
      let mut decoder = create_test_decoder::<T>(-1, enc);
      let mut result_data = vec![T::T::default(); total];
      decoder.set_data(data, total)?;
      let _ = decoder.decode(&mut result_data, total)?;

      assert_eq!(result_data, values);
      Ok(())
    }
  }

  impl EncodingTester<FixedLenByteArrayType> for FixedLenByteArrayType {
    fn test_internal(enc: Encoding, total: usize) -> Result<()> {
      let mut encoder = create_test_encoder::<FixedLenByteArrayType>(100, enc);
      let values = <FixedLenByteArrayType as RandGen<FixedLenByteArrayType>>::gen_vec(100, total);
      encoder.put(&values[..], total)?;

      let data = encoder.consume_buffer();
      let mut decoder = create_test_decoder::<FixedLenByteArrayType>(100, enc);
      let mut result_data = vec![ByteArray::default(); total];
      decoder.set_data(data, total)?;
      let _ = decoder.decode(&mut result_data, total)?;

      assert_eq!(result_data, values);
      Ok(())
    }
  }

  fn create_test_col_desc(type_len: i32, t: Type) -> ColumnDescriptor {
    let ty = SchemaType::new_primitive_type(
      "t", Repetition::OPTIONAL, t, LogicalType::NONE, type_len, 0, 0, None).unwrap();
    ColumnDescriptor::new(Rc::new(ty), None, 0, 0, ColumnPath::new(vec!()))
  }

  fn create_test_encoder<T: DataType>(type_len: i32, enc: Encoding) -> Box<Encoder<T>> where T: 'static {
    let desc = create_test_col_desc(type_len, T::get_physical_type());
    let encoder = match enc {
      Encoding::PLAIN => {
        let writer = BufWriter::new(ByteBuffer::new());
        Box::new(PlainEncoder::<T>::new(writer, Rc::new(desc)))
      }
      _ => {
        panic!("Not implemented yet.");
      }
    };
    encoder
  }

  fn create_test_decoder<T: DataType>(type_len: i32, enc: Encoding) -> Box<Decoder<T>> where T: 'static {
    let desc = create_test_col_desc(type_len, T::get_physical_type());
    let decoder = match enc {
      Encoding::PLAIN => {
        Box::new(PlainDecoder::<T>::new(desc.type_length()))
      },
      _ => {
        panic!("Not implemented yet.");
      }
    };
    decoder
  }
}
