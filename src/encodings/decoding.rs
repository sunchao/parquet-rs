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

use std::cmp;
use std::mem;
use std::marker::PhantomData;
use std::slice::from_raw_parts_mut;
use basic::*;
use data_type::*;
use errors::{Result, ParquetError};
use schema::types::ColumnDescPtr;
use util::bit_util::BitReader;
use util::memory::{ByteBufferPtr, ByteBuffer};
use super::rle_encoding::RleDecoder;


// ----------------------------------------------------------------------
// Decoders

pub trait Decoder<T: DataType> {
  /// Set the data to decode to be `data`, which should contain `num_values` of
  /// values to decode.
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()>;

  /// Try to consume values from this decoder and write
  /// the results to `buffer`. This will try to fill up `buffer`.
  /// Return the actual number of values written, which should be equal to
  /// `buffer.len()` unless the remaining number of values is less
  /// than `buffer.len()`.
  fn get(&mut self, buffer: &mut [T::T]) -> Result<usize>;

  /// Number of values left in this decoder stream
  fn values_left(&self) -> usize;

  /// Return the encoding for this decoder
  fn encoding(&self) -> Encoding;
}


/// Get a decoder for the particular data type `T` and encoding `encoding`.
/// `descr` and `value_type` currently are only used when getting a `RleDecoder`
/// and is used to decide whether this is for definition level or repetition level.
pub fn get_decoder<T: DataType>(descr: ColumnDescPtr,
                                encoding: Encoding) ->
    Result<Box<Decoder<T>>> where T: 'static {
  let decoder = match encoding {
    // TODO: why Rust cannot infer result type without the `as Box<...>`?
    Encoding::PLAIN => Box::new(PlainDecoder::new(descr.type_length())) as Box<Decoder<T>>,
    Encoding::DELTA_BINARY_PACKED => Box::new(DeltaBitPackDecoder::new()),
    Encoding::DELTA_LENGTH_BYTE_ARRAY => Box::new(DeltaLengthByteArrayDecoder::new()),
    Encoding::DELTA_BYTE_ARRAY => Box::new(DeltaByteArrayDecoder::new()),
    Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
      return Err(general_err!("Cannot initialize this encoding through this function"))
    },
    e => return Err(nyi_err!("Encoding {} is not supported.", e))
  };
  Ok(decoder)
}


// ----------------------------------------------------------------------
// PLAIN Decoding

pub struct PlainDecoder<T: DataType> {
  // The remaining number of values in the byte array
  num_values: usize,

  // The current starting index in the byte array.
  start: usize,

  // The length for the type `T`. Only used when `T` is `FixedLenByteArrayType`
  type_length: i32,

  // The byte array to decode from. Not set if `T` is bool.
  data: Option<ByteBufferPtr>,

  // Read `data` bit by bit. Only set if `T` is bool.
  bit_reader: Option<BitReader>,

  // To allow `T` in the generic parameter for this struct. This doesn't take any space.
  _phantom: PhantomData<T>
}

impl<T: DataType> PlainDecoder<T> {
  pub fn new(type_length: i32) -> Self {
    PlainDecoder {
      data: None, bit_reader: None, type_length: type_length,
      num_values: 0, start: 0, _phantom: PhantomData
    }
  }
}

default impl<T: DataType> Decoder<T> for PlainDecoder<T> {
  #[inline]
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    self.num_values = num_values;
    self.start = 0;
    self.data = Some(data);
    Ok(())
  }

  #[inline]
  fn values_left(&self) -> usize {
    self.num_values
  }

  #[inline]
  fn encoding(&self) -> Encoding {
    Encoding::PLAIN
  }

  #[inline]
  fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = mem::size_of::<T::T>() * num_values;
    if bytes_left < bytes_to_decode {
      return Err(general_err!("Not enough bytes to decode"));
    }
    let raw_buffer: &mut [u8] = unsafe {
      from_raw_parts_mut(buffer.as_ptr() as *mut u8, bytes_to_decode)
    };
    raw_buffer.copy_from_slice(data.range(self.start, bytes_to_decode).as_ref());
    self.start += bytes_to_decode;
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl Decoder<Int96Type> for PlainDecoder<Int96Type> {
  fn get(&mut self, buffer: &mut [Int96]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    let bytes_left = data.len() - self.start;
    let bytes_to_decode = 12 * num_values;
    if bytes_left < bytes_to_decode {
      return Err(general_err!("Not enough bytes to decode"));
    }
    for i in 0..num_values {
      buffer[i].set_data(
        unsafe {
          // TODO: avoid this copying
          let slice = ::std::slice::from_raw_parts(
            data.range(self.start, 12).as_ref().as_ptr() as *mut u32, 3);
          Vec::from(slice)
        }
      );
      self.start += 12;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl Decoder<BoolType> for PlainDecoder<BoolType> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    self.num_values = num_values;
    self.bit_reader = Some(BitReader::new(data));
    Ok(())
  }

  fn get(&mut self, buffer: &mut [bool]) -> Result<usize> {
    assert!(self.bit_reader.is_some());

    let mut bit_reader = self.bit_reader.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      bit_reader.get_value::<bool>(1).map(|b| {
        buffer[i] = b;
      })?;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl Decoder<ByteArrayType> for PlainDecoder<ByteArrayType> {
  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      let len: usize = read_num_bytes!(u32, 4, data.start_from(self.start).as_ref()) as usize;
      self.start += mem::size_of::<u32>();
      if data.len() < self.start + len {
        return Err(general_err!("Not enough bytes to decode"));
      }
      buffer[i].set_data(data.range(self.start, len));
      self.start += len;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}

impl Decoder<FixedLenByteArrayType> for PlainDecoder<FixedLenByteArrayType> {
  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.data.is_some());
    assert!(self.type_length > 0);

    let data = self.data.as_mut().unwrap();
    let type_length = self.type_length as usize;
    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      if data.len() < self.start + type_length {
        return Err(general_err!("Not enough bytes to decode"));
      }
      buffer[i].set_data(data.range(self.start, type_length));
      self.start += type_length;
    }
    self.num_values -= num_values;

    Ok(num_values)
  }
}


// ----------------------------------------------------------------------
// PLAIN_DICTIONARY Decoding

pub struct DictDecoder<T: DataType> {
  // The dictionary, which maps ids to the values
  dictionary: Vec<T::T>,

  // Whether `dictionary` has been initialized
  has_dictionary: bool,

  // The decoder for the value ids
  rle_decoder: Option<RleDecoder>,

  // Number of values left in the data stream
  num_values: usize,
}

impl<T: DataType> DictDecoder<T> {
  pub fn new() -> Self {
    Self { dictionary: vec!(), has_dictionary: false, rle_decoder: None, num_values: 0 }
  }

  pub fn set_dict(&mut self, mut decoder: Box<Decoder<T>>) -> Result<()> {
    let num_values = decoder.values_left();
    self.dictionary.resize(num_values, T::T::default());
    let _ = decoder.get(&mut self.dictionary)?;
    self.has_dictionary = true;
    Ok(())
  }
}

impl<T: DataType> Decoder<T> for DictDecoder<T> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    // first byte in `data` is bit width
    let bit_width = data.as_ref()[0] as usize;
    let mut rle_decoder = RleDecoder::new(bit_width);
    rle_decoder.set_data(data.start_from(1));
    self.num_values = num_values;
    self.rle_decoder = Some(rle_decoder);
    Ok(())
  }

  fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
    assert!(self.rle_decoder.is_some());
    assert!(self.has_dictionary, "Must call set_dict() first!");

    let mut rle = self.rle_decoder.as_mut().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    rle.get_batch_with_dict(&self.dictionary[..], buffer, num_values)
  }

  /// Number of values left in this decoder stream
  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::PLAIN_DICTIONARY
  }
}


// ----------------------------------------------------------------------
// DELTA_BINARY_PACKED Decoding

pub struct DeltaBitPackDecoder<T: DataType> {
  bit_reader: Option<BitReader>,

  // Header info
  num_values: usize,
  num_mini_blocks: i64,
  values_per_mini_block: i64,
  values_current_mini_block: i64,
  first_value: i64,
  first_value_read: bool,

  // Per block info
  min_delta: i64,
  mini_block_idx: usize,
  delta_bit_width: u8,
  delta_bit_widths: ByteBuffer,

  current_value: i64,

  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaBitPackDecoder<T> {
  pub fn new() -> Self {
    Self { bit_reader: None, num_values: 0, num_mini_blocks: 0,
           values_per_mini_block: 0, values_current_mini_block: 0,
           first_value: 0, first_value_read: false,
           min_delta: 0, mini_block_idx: 0, delta_bit_width: 0,
           delta_bit_widths: ByteBuffer::new(),
           current_value: 0, _phantom: PhantomData }
  }

  pub fn get_offset(&self) -> usize {
    assert!(self.bit_reader.is_some());
    let reader = self.bit_reader.as_ref().unwrap();
    reader.get_byte_offset()
  }

  #[inline]
  fn init_block(&mut self) -> Result<()> {
    assert!(self.bit_reader.is_some());
    let mut bit_reader = self.bit_reader.as_mut().unwrap();

    self.min_delta = bit_reader.get_zigzag_vlq_int()?;
    let mut widths = vec!();
    for _ in 0..self.num_mini_blocks {
      let w = bit_reader.get_aligned::<u8>(1)?;
      widths.push(w);
    }
    self.delta_bit_widths.set_data(widths);
    self.mini_block_idx = 0;
    self.values_current_mini_block = self.values_per_mini_block;
    self.current_value = self.first_value; // TODO: double check this
    Ok(())
  }
}

default impl<T: DataType> Decoder<T> for DeltaBitPackDecoder<T> {
  fn set_data(&mut self, _: ByteBufferPtr, _: usize) -> Result<()> {
    Err(general_err!("DeltaBitPackDecoder only support Int64Type"))
  }

  fn get(&mut self, _: &mut [T::T]) -> Result<usize> {
    Err(general_err!("DeltaBitPackDecoder only support Int64Type"))
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_BINARY_PACKED
  }
}

impl Decoder<Int64Type> for DeltaBitPackDecoder<Int64Type> {
  // # of total values is derived from encoding
  #[inline]
  fn set_data(&mut self, data: ByteBufferPtr, _: usize) -> Result<()> {
    let mut bit_reader = BitReader::new(data);

    let block_size = bit_reader.get_vlq_int()?;
    self.num_mini_blocks = bit_reader.get_vlq_int()?;
    self.num_values = bit_reader.get_vlq_int()? as usize;
    self.first_value = bit_reader.get_zigzag_vlq_int()?;
    self.first_value_read = false;
    self.values_per_mini_block = (block_size / self.num_mini_blocks) as i64;
    assert!(self.values_per_mini_block % 8 == 0);

    self.bit_reader = Some(bit_reader);
    Ok(())
  }

  // TODO: same impl for i32?
  #[inline]
  fn get(&mut self, buffer: &mut [i64]) -> Result<usize> {
    assert!(self.bit_reader.is_some());

    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      if !self.first_value_read {
        buffer[i] = self.first_value;
        self.first_value_read = true;
        continue;
      }

      if self.values_current_mini_block == 0 {
        self.mini_block_idx += 1;
        if self.mini_block_idx < self.delta_bit_widths.size() {
          self.delta_bit_width = self.delta_bit_widths.data()[self.mini_block_idx];
          self.values_current_mini_block = self.values_per_mini_block;
        } else {
          self.init_block()?;
        }
      }

      // we can't move this outside the loop since it needs to
      // "carve out" the permission of `self.bit_reader` which makes
      // `self.init_block()` call impossible (the latter requires the
      // whole permission of `self`)
      // TODO: evaluate the performance impact of this.
      let bit_reader = self.bit_reader.as_mut().unwrap();

      // TODO: use SIMD to optimize this?
      let delta = bit_reader.get_value(self.delta_bit_width as usize)?;
      self.current_value += self.min_delta;
      self.current_value += delta;
      buffer[i] = self.current_value;
      self.values_current_mini_block -= 1;
    }

    self.num_values -= num_values;
    Ok(num_values)
  }
}


// ----------------------------------------------------------------------
// DELTA_LENGTH_BYTE_ARRAY Decoding

pub struct DeltaLengthByteArrayDecoder<T: DataType> {
  // Lengths for each byte array in `data`
  // TODO: add memory tracker to this
  lengths: Vec<i64>,

  // Current index into `lengths`
  current_idx: usize,

  // Concatenated byte array data
  data: Option<ByteBufferPtr>,

  // Offset into `data`, always point to the beginning of next byte array.
  offset: usize,

  // Number of values left in this decoder stream
  num_values: usize,

  // Placeholder to allow `T` as generic parameter
  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaLengthByteArrayDecoder<T> {
  pub fn new() -> Self {
    Self { lengths: vec!(), current_idx: 0, data: None, offset: 0, num_values: 0, _phantom: PhantomData }
  }
}

default impl<T: DataType> Decoder<T> for DeltaLengthByteArrayDecoder<T> {
  fn set_data(&mut self, _: ByteBufferPtr, _: usize) -> Result<()> {
    Err(general_err!("DeltaLengthByteArrayDecoder only support ByteArrayType"))
  }

  fn get(&mut self, _: &mut [T::T]) -> Result<usize> {
    Err(general_err!("DeltaLengthByteArrayDecoder only support ByteArrayType"))
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_LENGTH_BYTE_ARRAY
  }
}

impl Decoder<ByteArrayType> for DeltaLengthByteArrayDecoder<ByteArrayType> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    let mut len_decoder = DeltaBitPackDecoder::new();
    len_decoder.set_data(data.all(), num_values)?;
    let num_lengths = len_decoder.values_left();
    self.lengths.resize(num_lengths, 0);
    len_decoder.get(&mut self.lengths[..])?;

    self.data = Some(data.start_from(len_decoder.get_offset()));
    self.offset = 0;
    self.current_idx = 0;
    self.num_values = num_lengths;
    Ok(())
  }

  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.data.is_some());

    let data = self.data.as_ref().unwrap();
    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      let len = self.lengths[self.current_idx] as usize;
      buffer[i].set_data(data.range(self.offset, len));
      self.offset += len;
      self.current_idx += 1;
    }

    self.num_values -= num_values;
    Ok(num_values)
  }
}

// ----------------------------------------------------------------------
// DELTA_BYTE_ARRAY Decoding

pub struct DeltaByteArrayDecoder<T: DataType> {
  // Prefix lengths for each byte array
  // TODO: add memory tracker to this
  prefix_lengths: Vec<i64>,

  // The current index into `prefix_lengths`,
  current_idx: usize,

  // Decoder for all suffixs, the # of which should be the same as `prefix_lengths.len()`
  suffix_decoder: Option<DeltaLengthByteArrayDecoder<ByteArrayType>>,

  // The last byte array, used to derive the current prefix
  previous_value: Option<ByteBufferPtr>,

  // Number of values left
  num_values: usize,

  // Placeholder to allow `T` as generic parameter
  _phantom: PhantomData<T>
}

impl<T: DataType> DeltaByteArrayDecoder<T> {
  pub fn new() -> Self {
    Self { prefix_lengths: vec!(), current_idx: 0, suffix_decoder: None,
           previous_value: None, num_values: 0, _phantom: PhantomData }
  }
}

default impl<'m, T: DataType> Decoder<T> for DeltaByteArrayDecoder<T> {
  fn set_data(&mut self, _: ByteBufferPtr, _: usize) -> Result<()> {
    Err(general_err!("DeltaLengthByteArrayDecoder only support ByteArrayType"))
  }

  fn get(&mut self, _: &mut [T::T]) -> Result<usize> {
    Err(general_err!("DeltaByteArrayDecoder only support ByteArrayType"))
  }

  fn values_left(&self) -> usize {
    self.num_values
  }

  fn encoding(&self) -> Encoding {
    Encoding::DELTA_BYTE_ARRAY
  }
}

impl<> Decoder<ByteArrayType> for DeltaByteArrayDecoder<ByteArrayType> {
  fn set_data(&mut self, data: ByteBufferPtr, num_values: usize) -> Result<()> {
    let mut prefix_len_decoder = DeltaBitPackDecoder::new();
    prefix_len_decoder.set_data(data.all(), num_values)?;
    let num_prefixes = prefix_len_decoder.values_left();
    self.prefix_lengths.resize(num_prefixes, 0);
    prefix_len_decoder.get(&mut self.prefix_lengths[..])?;

    let mut suffix_decoder = DeltaLengthByteArrayDecoder::new();
    suffix_decoder.set_data(data.start_from(prefix_len_decoder.get_offset()), num_values)?;
    self.suffix_decoder = Some(suffix_decoder);
    self.num_values = num_prefixes;
    Ok(())
  }

  fn get(&mut self, buffer: &mut [ByteArray]) -> Result<usize> {
    assert!(self.suffix_decoder.is_some());

    let num_values = cmp::min(buffer.len(), self.num_values);
    for i in 0..num_values {
      // Process prefix
      let mut prefix_slice: Option<Vec<u8>> = None;
      let prefix_len = self.prefix_lengths[self.current_idx];
      if prefix_len != 0 {
        assert!(self.previous_value.is_some());
        let previous = self.previous_value.as_ref().unwrap();
        prefix_slice = Some(Vec::from(previous.as_ref()));
      }
      // process suffix
      // TODO: this is awkward - maybe we should add a non-vectorized API?
      let mut suffix = vec![ByteArray::new(); 1];
      let suffix_decoder = self.suffix_decoder.as_mut().unwrap();
      suffix_decoder.get(&mut suffix[..])?;

      // Concatenate prefix with suffix
      let result: Vec<u8> = match prefix_slice {
        Some(mut prefix) => {
          prefix.extend_from_slice(suffix[0].data());
          prefix
        }
        None => Vec::from(suffix[0].data())
      };

      // TODO: can reuse the original vec
      let data = ByteBufferPtr::new(result);
      buffer[i].set_data(data.all());
      self.previous_value = Some(data);
    }

    self.num_values -= num_values;
    Ok(num_values)
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use std::mem;
  use util::bit_util::set_array_bit;

  #[test]
  fn test_plain_decode_int32() {
    let data = vec![42, 18, 52];
    let data_bytes = Int32Type::to_byte_array(&data[..]);
    let mut buffer = vec![0; 3];
    test_plain_decode::<Int32Type>(ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_int64() {
    let data = vec![42, 18, 52];
    let data_bytes = Int64Type::to_byte_array(&data[..]);
    let mut buffer = vec![0; 3];
    test_plain_decode::<Int64Type>(ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]);
  }


  #[test]
  fn test_plain_decode_float() {
    let data = vec![3.14, 2.414, 12.51];
    let data_bytes = FloatType::to_byte_array(&data[..]);
    let mut buffer = vec![0.0; 3];
    test_plain_decode::<FloatType>(ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_double() {
    let data = vec![3.14f64, 2.414f64, 12.51f64];
    let data_bytes = DoubleType::to_byte_array(&data[..]);
    let mut buffer = vec![0.0f64; 3];
    test_plain_decode::<DoubleType>(ByteBufferPtr::new(data_bytes), 3, -1, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_int96() {
    let v0 = vec![11, 22, 33];
    let v1 = vec![44, 55, 66];
    let v2 = vec![10, 20, 30];
    let v3 = vec![40, 50, 60];
    let mut data = vec![Int96::new(); 4];
    data[0].set_data(v0);
    data[1].set_data(v1);
    data[2].set_data(v2);
    data[3].set_data(v3);
    let data_bytes = Int96Type::to_byte_array(&data[..]);
    let mut buffer = vec![Int96::new(); 4];
    test_plain_decode::<Int96Type>(ByteBufferPtr::new(data_bytes), 4, -1, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_bool() {
    let data = vec![false, true, false, false, true, false, true, true, false, true];
    let data_bytes = BoolType::to_byte_array(&data[..]);
    let mut buffer = vec![false; 10];
    test_plain_decode::<BoolType>(ByteBufferPtr::new(data_bytes), 10, -1, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_byte_array() {
    let mut data = vec!(ByteArray::new(); 2);
    data[0].set_data(ByteBufferPtr::new(String::from("hello").into_bytes()));
    data[1].set_data(ByteBufferPtr::new(String::from("parquet").into_bytes()));
    let data_bytes = ByteArrayType::to_byte_array(&data[..]);
    let mut buffer = vec![ByteArray::new(); 2];
    test_plain_decode::<ByteArrayType>(ByteBufferPtr::new(data_bytes), 2, -1, &mut buffer[..], &data[..]);
  }

  #[test]
  fn test_plain_decode_fixed_len_byte_array() {
    let mut data = vec!(ByteArray::default(); 3);
    data[0].set_data(ByteBufferPtr::new(String::from("bird").into_bytes()));
    data[1].set_data(ByteBufferPtr::new(String::from("come").into_bytes()));
    data[2].set_data(ByteBufferPtr::new(String::from("flow").into_bytes()));
    let data_bytes = FixedLenByteArrayType::to_byte_array(&data[..]);
    let mut buffer = vec![ByteArray::default(); 3];
    test_plain_decode::<FixedLenByteArrayType>(ByteBufferPtr::new(data_bytes), 3, 4, &mut buffer[..], &data[..]);
  }

  fn test_plain_decode<T: DataType>(data: ByteBufferPtr,
                                    num_values: usize,
                                    type_length: i32,
                                    buffer: &mut [T::T],
                                    expected: &[T::T]) {
    let mut decoder: PlainDecoder<T> = PlainDecoder::new(type_length);
    let result = decoder.set_data(data, num_values);
    assert!(result.is_ok());
    let result = decoder.get(&mut buffer[..]);
    assert!(result.is_ok());
    assert_eq!(decoder.values_left(), 0);
    assert_eq!(buffer, expected);
  }

  fn usize_to_bytes(v: usize) -> [u8; 4] {
    unsafe { mem::transmute::<u32, [u8; 4]>(v as u32) }
  }

  /// A util trait to convert slices of different types to byte arrays
  trait ToByteArray<T: DataType> {
    fn to_byte_array(data: &[T::T]) -> Vec<u8>;
  }

  default impl<T> ToByteArray<T> for T where T: DataType {
    fn to_byte_array(data: &[T::T]) -> Vec<u8> {
      let mut v = vec!();
      let type_len = ::std::mem::size_of::<T::T>();
      v.extend_from_slice(
        unsafe {
          ::std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * type_len)
        }
      );
      v
    }
  }

  impl ToByteArray<BoolType> for BoolType {
    fn to_byte_array(data: &[bool]) -> Vec<u8> {
      let mut v = vec!();
      for i in 0..data.len() {
        if i % 8 == 0 {
          v.push(0);
        }
        if data[i] {
          set_array_bit(&mut v[..], i);
        }
      }
      v
    }
  }

  impl ToByteArray<Int96Type> for Int96Type {
    fn to_byte_array(data: &[Int96]) -> Vec<u8> {
      let mut v = vec!();
      for d in data {
        unsafe {
          let copy = ::std::slice::from_raw_parts(d.data().as_ptr() as *const u8, 12);
          v.extend_from_slice(copy);
        };
      }
      v
    }
  }

  impl ToByteArray<ByteArrayType> for ByteArrayType {
    fn to_byte_array(data: &[ByteArray]) -> Vec<u8> {
      let mut v = vec!();
      for d in data {
        let buf = d.data();
        let len = &usize_to_bytes(buf.len());
        v.extend_from_slice(len);
        v.extend(buf);
      }
      v
    }
  }

  impl ToByteArray<FixedLenByteArrayType> for FixedLenByteArrayType {
    fn to_byte_array(data: &[ByteArray]) -> Vec<u8> {
      let mut v = vec!();
      for d in data {
        let buf = d.data();
        v.extend(buf);
      }
      v
    }
  }
}
