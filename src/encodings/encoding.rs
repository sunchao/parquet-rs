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
use data_type::*;
use errors::{Result};
use schema::types::ColumnDescPtr;
use util::memory::{ByteBufferPtr, ByteBuffer, Buffer, MemTrackerPtr};
use util::bit_util::{BitWriter, convert_to_bytes, ceil, log2, memcpy};
use util::hash_util::{self};
use encodings::rle_encoding::RawRleEncoder;

pub trait Encoder<T: DataType> {
  /// Encode `num_values` of values from `src`.
  fn put(&mut self, src: &[T::T], num_values: usize) -> Result<()>;

  /// Return the encoding type of this encoder.
  fn encoding(&self) -> Encoding;

  /// Consume the underlying byte buffer that's being processed
  /// by this encoder, and return it.
  fn consume_buffer(&mut self) -> Result<ByteBufferPtr>;
}


// ----------------------------------------------------------------------
// Plain encoding

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

default impl<T: DataType> Encoder<T> for PlainEncoder<T> {
  fn put(&mut self, src: &[T::T], num_values: usize) -> Result<()> {
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

  fn consume_buffer(&mut self) -> Result<ByteBufferPtr> {
    Ok(self.out.get_mut().consume())
  }
}

impl Encoder<BoolType> for PlainEncoder<BoolType> {
  fn put(&mut self, src: &[bool], num_values: usize) -> Result<()> {
    assert!(src.len() >= num_values);
    for i in 0..num_values {
      self.bit_writer.put_value(src[i] as u64, 1);
    }

    // TODO: maybe we should write directly to `out`?
    self.bit_writer.flush();
    self.out.write(self.bit_writer.buffer())?;
    self.out.flush()?;
    self.bit_writer.clear();
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


// ----------------------------------------------------------------------
// Dictionary encoding

const INITIAL_HASH_TABLE_SIZE: usize = 1024;
const MAX_HASH_LOAD: f32 = 0.7;
const HASH_SLOT_EMPTY: i32 = -1;

pub trait DictEncoderTrait<T: DataType>: Encoder<T> {
  /// Write out all unique values encountered in PLAIN encoding.
  fn write_dict(&self) -> ByteBufferPtr;

  /// Write out all buffered indices to a buffer where the first byte stores
  /// the bit width of the data. Return the fully written buffer.
  fn write_indices(&mut self) -> Result<ByteBufferPtr>;
}

pub struct DictEncoder<T: DataType> {
  /// Descriptor for the column to be encoded.
  desc: ColumnDescPtr,

  /// Size of the table. **Must be** a power of 2.
  hash_table_size: usize,

  /// Store `hash_table_size` - 1, so that `j & mod_bitmask` is equivalent to
  /// `j % hash_table_size`, but uses far fewer CPU cycles.
  mod_bitmask: u64,

  /// Stores indices which map (many-to-one) to the values in the `uniques` array.
  /// Here we are using fix-sized array with linear probing.
  /// A slot with `HASH_SLOT_EMPTY` indicates the slot is not currently occupied.
  hash_slots: Buffer<i32>,

  /// Indices that have not yet be written out by `write_indices()`.
  buffered_indices: Buffer<i32>,

  /// The unique observed values.
  uniques: Buffer<T::T>,

  /// The number of bytes needed to encode this dictionary
  dict_encoded_size: u64,

  /// Tracking memory usage for the various data structures in this struct.
  mem_tracker: MemTrackerPtr
}

default impl<T: DataType> DictEncoder<T> {
  pub fn new(desc: ColumnDescPtr, mem_tracker: MemTrackerPtr) -> Self {
    let mut slots = Buffer::new().with_mem_tracker(mem_tracker.clone());
    slots.resize(INITIAL_HASH_TABLE_SIZE, -1);
    Self {
      desc: desc,
      hash_table_size: INITIAL_HASH_TABLE_SIZE,
      mod_bitmask: (INITIAL_HASH_TABLE_SIZE - 1) as u64,
      hash_slots: slots,
      buffered_indices: Buffer::new().with_mem_tracker(mem_tracker.clone()),
      uniques: Buffer::new().with_mem_tracker(mem_tracker.clone()),
      dict_encoded_size: 0,
      mem_tracker: mem_tracker
    }
  }

  pub fn num_entries(&self) -> usize {
    self.uniques.size()
  }

  #[inline]
  fn put_one(&mut self, value: &T::T) -> Result<()> {
    let mut j = (hash_util::hash(value, 0) & self.mod_bitmask) as usize;
    let mut index = self.hash_slots[j];

    while index != HASH_SLOT_EMPTY && self.uniques[index as usize] != *value {
      j += 1;
      if j == self.hash_table_size {
        j = 0;
      }
      index = self.hash_slots[j];
    }

    if index == HASH_SLOT_EMPTY {
      index = self.uniques.size() as i32;
      self.hash_slots[j] = index;
      self.add_dict_key(value.clone());

      if self.uniques.size() > (self.hash_table_size as f32 * MAX_HASH_LOAD) as usize {
        self.double_table_size();
      }
    }

    self.buffered_indices.push(index);
    Ok(())
  }

  #[inline]
  fn add_dict_key(&mut self, value: T::T) {
    self.uniques.push(value);
    self.dict_encoded_size += ::std::mem::size_of::<T::T>() as u64;
  }

  #[inline]
  fn bit_width(&self) -> usize {
    let num_entries = self.uniques.size();
    log2(num_entries as u64) as usize
  }

  #[inline]
  fn double_table_size(&mut self) {
    let new_size = self.hash_table_size * 2;
    let mut new_hash_slots = Buffer::new().with_mem_tracker(self.mem_tracker.clone());
    new_hash_slots.resize(new_size, HASH_SLOT_EMPTY);
    for i in 0..self.hash_table_size {
      let index = self.hash_slots[i];
      if index == HASH_SLOT_EMPTY {
        continue;
      }
      let value = &self.uniques[index as usize];
      let mut j = (hash_util::hash(value, 0) & ((new_size - 1) as u64)) as usize;
      let mut slot = new_hash_slots[j];
      while slot != HASH_SLOT_EMPTY && self.uniques[slot as usize] != *value {
        j += 1;
        if j == new_size {
          j == 0;
        }
        slot = new_hash_slots[j];
      }

      new_hash_slots[j] = index;
    }

    self.hash_table_size = new_size;
    self.mod_bitmask = (new_size - 1) as u64;
    ::std::mem::replace(&mut self.hash_slots, new_hash_slots);
  }
}

default impl<T: DataType> Encoder<T> for DictEncoder<T> {
  #[inline]
  fn put(&mut self, src: &[T::T], num_values: usize) -> Result<()> {
    for i in 0..num_values {
      self.put_one(&src[i])?
    }
    Ok(())
  }

  fn encoding(&self) -> Encoding {
    Encoding::PLAIN_DICTIONARY
  }

  #[inline]
  fn consume_buffer(&mut self) -> Result<ByteBufferPtr> {
    self.write_indices()
  }
}

default impl<T: DataType> DictEncoderTrait<T> for DictEncoder<T> {
  #[inline]
  fn write_dict(&self) -> ByteBufferPtr {
    let capacity = self.uniques.size() * ::std::mem::size_of::<T::T>();
    self.mem_tracker.alloc(capacity as i64);
    let mut buf: Vec<u8> = vec![0; capacity];
    self.mem_tracker.alloc(buf.capacity() as i64);
    // TODO: extract this into a function like `bit_util::memcpy`
    unsafe {
      ::std::ptr::copy_nonoverlapping(
        self.uniques.data().as_ptr() as *const T::T,
        buf.as_mut_ptr() as *mut u8 as *mut T::T,
        self.uniques.size())
    }
    ByteBufferPtr::new(buf).with_mem_tracker(self.mem_tracker.clone())
  }

  #[inline]
  fn write_indices(&mut self) -> Result<ByteBufferPtr> {
    let bit_width = self.bit_width();
    let buffer_len = 1 + RawRleEncoder::min_buffer_size(bit_width);
    let mut buffer: Vec<u8> = vec![0; buffer_len as usize];
    buffer[0] = bit_width as u8;
    self.mem_tracker.alloc(buffer.capacity() as i64);

    // Write bit width in the first byte
    buffer.write((self.bit_width() as u8).as_bytes())?;
    let mut encoder = RawRleEncoder::new_from_buf(self.bit_width(), buffer, 1);
    for index in self.buffered_indices.data() {
      let success = encoder.put(*index as u64)?;
      assert!(success, "Buffer should have enough capacity");
    }
    self.buffered_indices.clear();
    Ok(ByteBufferPtr::new(encoder.consume()?))
  }
}

impl DictEncoderTrait<BoolType> for DictEncoder<BoolType> {
  #[inline]
  fn write_dict(&self) -> ByteBufferPtr {
    let capacity = ceil(self.uniques.size() as i64, 8) as usize;
    let buf: Vec<u8> = vec![0; capacity];
    self.mem_tracker.alloc(buf.capacity() as i64);
    let mut bit_writer = BitWriter::new_from_buf(buf, 0);
    for v in self.uniques.data() {
      let success = bit_writer.put_value(*v as u64, 1);
      assert!(success, "Not enough room in bit writer");
    }
    let result = bit_writer.consume();
    ByteBufferPtr::new(result).with_mem_tracker(self.mem_tracker.clone())
  }
}

impl DictEncoderTrait<Int96Type> for DictEncoder<Int96Type> {
  #[inline]
  fn write_dict(&self) -> ByteBufferPtr {
    let capacity = self.uniques.size() * 12;
    self.mem_tracker.alloc(capacity as i64);
    let mut buf: Vec<u8> = vec![0; capacity];
    self.mem_tracker.alloc(buf.capacity() as i64);
    for (i, v) in self.uniques.data().iter().enumerate() {
      memcpy(v.as_bytes(), &mut buf[i * 12..]);
    }
    ByteBufferPtr::new(buf).with_mem_tracker(self.mem_tracker.clone())
  }
}

impl DictEncoderTrait<ByteArrayType> for DictEncoder<ByteArrayType> {
  #[inline]
  fn write_dict(&self) -> ByteBufferPtr {
    let mut buf: Vec<u8> = vec!();
    for v in self.uniques.data() {
      let len = v.len();
      self.mem_tracker.alloc((::std::mem::size_of::<u32>() + len) as i64);
      buf.extend_from_slice((len.to_le() as u32).as_bytes());
      buf.extend_from_slice(v.data());
    }
    ByteBufferPtr::new(buf).with_mem_tracker(self.mem_tracker.clone())
  }
}

impl DictEncoderTrait<FixedLenByteArrayType> for DictEncoder<FixedLenByteArrayType> {
  #[inline]
  fn write_dict(&self) -> ByteBufferPtr {
    let type_len = self.desc.type_length() as usize;
    let mut buf = ByteBuffer::new().with_mem_tracker(self.mem_tracker.clone());
    buf.resize(type_len * self.uniques.size(), 0);
    for (i, v) in self.uniques.data().iter().enumerate() {
      memcpy(v.data(), &mut buf.mut_data()[(i * type_len)..])
    }
    buf.consume()
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use super::super::decoding::*;
  use std::rc::Rc;
  use schema::types::{Type as SchemaType, ColumnDescriptor, ColumnPath};
  use util::memory::MemTracker;
  use util::test_common::RandGen;

  const TEST_SET_SIZE: usize = 32;

  #[test]
  fn test_bool() {
    BoolType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    BoolType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_i32() {
    Int32Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    Int32Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_i64() {
    Int64Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    Int64Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_i96() {
    Int96Type::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    Int96Type::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_float() {
    FloatType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    FloatType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_double() {
    DoubleType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    DoubleType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_byte_array() {
    ByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, -1);
    ByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, -1);
  }

  #[test]
  fn test_fixed_lenbyte_array() {
    FixedLenByteArrayType::test(Encoding::PLAIN, TEST_SET_SIZE, 100);
    FixedLenByteArrayType::test(Encoding::PLAIN_DICTIONARY, TEST_SET_SIZE, 100);
  }

  trait EncodingTester<T: DataType> {
    fn test(enc: Encoding, total: usize, type_length: i32) {
      let result = match enc {
        Encoding::PLAIN_DICTIONARY => Self::test_dict_internal(total, type_length),
        enc @ _ => Self::test_internal(enc, total, type_length)
      };

      assert!(result.is_ok(), "Expected result to be OK but got err:\n {}", result.unwrap_err());
    }

    fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()>;

    fn test_dict_internal(total: usize, type_length: i32) -> Result<()>;
  }

  default impl<T: DataType> EncodingTester<T> for T where T: 'static {
    fn test_internal(enc: Encoding, total: usize, type_length: i32) -> Result<()> {
      let mut encoder = create_test_encoder::<T>(type_length, enc);
      let values = <T as RandGen<T>>::gen_vec(type_length, total);
      encoder.put(&values[..], total)?;

      let data = encoder.consume_buffer()?;
      let mut decoder = create_test_decoder::<T>(type_length, enc);
      let mut result_data = vec![T::T::default(); total];
      decoder.set_data(data, total)?;
      let _ = decoder.decode(&mut result_data, total)?;

      assert_eq!(result_data, values);
      Ok(())
    }

    fn test_dict_internal(total: usize, type_length: i32) -> Result<()> {
      let mut encoder = create_test_dict_encoder::<T>(type_length);
      let values = <T as RandGen<T>>::gen_vec(type_length, total);
      encoder.put(&values[..], total)?;

      let data = encoder.consume_buffer()?;
      let mut decoder = create_test_dict_decoder::<T>();
      let mut dict_decoder = PlainDecoder::<T>::new(type_length);
      dict_decoder.set_data(encoder.write_dict(), encoder.num_entries())?;
      decoder.set_dict(Box::new(dict_decoder))?;
      let mut result_data = vec![T::T::default(); total];
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
    let mem_tracker = MemTracker::new_ptr(None).unwrap();
    let encoder = match enc {
      Encoding::PLAIN => {
        let writer = BufWriter::new(ByteBuffer::new());
        Box::new(PlainEncoder::<T>::new(writer, Rc::new(desc)))
      },
      Encoding::PLAIN_DICTIONARY => {
        Box::new(DictEncoder::<T>::new(Rc::new(desc), mem_tracker)) as Box<Encoder<T>>
      },
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
      Encoding::PLAIN_DICTIONARY => {
        Box::new(DictDecoder::<T>::new()) as Box<Decoder<T>>
      },
      _ => {
        panic!("Not implemented yet.");
      }
    };
    decoder
  }

  fn create_test_dict_encoder<T: DataType>(type_len: i32) -> DictEncoder<T> {
    let desc = create_test_col_desc(type_len, T::get_physical_type());
    let mem_tracker = MemTracker::new_ptr(None).unwrap();
    DictEncoder::<T>::new(Rc::new(desc), mem_tracker)
  }

  fn create_test_dict_decoder<T: DataType>() -> DictDecoder<T> {
    DictDecoder::<T>::new()
  }
}
