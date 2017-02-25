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

use arena::TypedArena;

use errors::Result;

// ----------------------------------------------------------------------
// Buffer classes

/// Basic APIs for byte buffers. A byte buffer has two attributes:
/// `capacity` and `size`: the former is the total bytes allocated for
/// the buffer, while the latter is the actual bytes that have valid data.
/// Invariant: `capacity` >= `size`.
///
/// A `Buffer` is immutable, meaning that one can only obtain the
/// underlying data for read only
pub trait Buffer {
  /// Get a shared reference to the underlying data
  fn data(&self) -> &[u8];

  /// Get the capacity of this buffer
  fn capacity(&self) -> usize;

  /// Get the size for this buffer
  fn size(&self) -> usize;
}

/// A byte buffer where client can obtain a unique reference to
/// the underlying data for both read and write
pub trait MutableBuffer: Buffer {
  /// Get a unique reference to the underlying data
  fn mut_data(&mut self) -> &mut [u8];
}

/// A type of buffer where the underlying data can grow and shrink
pub trait ResizableBuffer: MutableBuffer {
  fn resize(&mut self, new_cap: usize) -> Result<()>;
}

pub struct ByteBuffer {
  data: Vec<u8>
}

impl ByteBuffer {
  pub fn new(size: usize) -> Self {
    let data = vec![0; size];
    ByteBuffer { data: data }
  }
}

impl Buffer for ByteBuffer {
  fn data(&self) -> &[u8] {
    self.data.as_slice()
  }

  fn capacity(&self) -> usize {
    self.data.capacity()
  }

  fn size(&self) -> usize {
    self.data.len()
  }
}

impl MutableBuffer for ByteBuffer {
  fn mut_data(&mut self) -> &mut [u8] {
    self.data.as_mut_slice()
  }
}


// ----------------------------------------------------------------------
// MemoryPool classes


/// A central place for managing memory.
/// NOTE: client can only acquire byte buffers through this API, but not releasing.
/// All the memory will be released once the instance of this trait goes out of scope.
pub trait MemoryPool {
  /// Acquire a new byte buffer of at least `size` bytes
  /// Return a unique reference to the buffer
  fn acquire(&mut self, size: usize) -> Result<&mut Box<Buffer>>;

  /// Consume `buf` and add it to this memory pool
  /// After the call, `buf` has the same lifetime as the pool.
  /// Return a unique reference to the consumed buffer.
  fn consume(&mut self, buf: Box<Buffer>) -> &mut Box<Buffer>;

  /// Return the total number of bytes allocated so far
  fn cur_allocated(&self) -> i64;

  /// Return the maximum number of bytes allocated so far
  fn max_allocated(&self) -> i64;
}

pub struct DefaultMemoryPool {
  arena: TypedArena<Box<Buffer>>,
  cur_bytes_allocated: i64,
  max_bytes_allocated: i64
}

impl DefaultMemoryPool {
  fn new() -> Self {
    let arena = TypedArena::new();
    DefaultMemoryPool{ arena: arena, cur_bytes_allocated: 0, max_bytes_allocated: 0 }
  }
}

impl MemoryPool for DefaultMemoryPool {
  fn acquire(&mut self, size: usize) -> Result<&mut Box<Buffer>> {
    let buf = Box::new(ByteBuffer::new(size));
    let result: &mut Box<Buffer> = self.arena.alloc(buf);
    Ok(result)
  }

  fn consume(&mut self, buf: Box<Buffer>) -> &mut Box<Buffer> {
    self.arena.alloc(buf)
  }

  fn cur_allocated(&self) -> i64 {
    self.cur_bytes_allocated
  }

  fn max_allocated(&self) -> i64 {
    self.max_bytes_allocated
  }
}
