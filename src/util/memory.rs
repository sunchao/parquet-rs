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

use std::cell::{RefCell, Cell};
use std::cmp;
use std::fmt::{Display, Result as FmtResult, Formatter};
use std::io::{Result as IoResult, Write};
use std::mem;
use std::rc::{Rc, Weak};

use arena::TypedArena;
use errors::Result;


// ----------------------------------------------------------------------
// Memory Tracker classes

type MemTrackerPtr = Rc<MemTracker>;
type WeakMemTrackerPtr = Weak<MemTracker>;

pub struct MemTracker {
  cur_bytes: Cell<i64>,
  max_bytes: Cell<i64>,
  children: RefCell<Vec<MemTrackerPtr>>,
  parent: Option<WeakMemTrackerPtr>
}

impl MemTracker {
  pub fn new_ptr(mut parent: Option<&mut MemTrackerPtr>) -> Result<MemTrackerPtr> {
    let mem_tracker = Rc::new(MemTracker {
      cur_bytes: Cell::new(0), max_bytes: Cell::new(0),
      children: RefCell::new(vec!()),
      parent: parent.as_mut().map(|p| Rc::downgrade(p))
    });
    if let Some(p) = parent {
      let mut parent_children = p.children.try_borrow_mut()?;
      parent_children.push(mem_tracker.clone());
    }
    Ok(mem_tracker)
  }

  pub fn cur_bytes(&self) -> i64 {
    self.cur_bytes.get()
  }

  pub fn max_bytes(&self) -> i64 {
    self.max_bytes.get()
  }

  pub fn alloc(&self, num_bytes: usize) {
    let new_cur_bytes = self.cur_bytes.get() + num_bytes as i64;
    if new_cur_bytes > self.max_bytes.get() {
      self.max_bytes.set(new_cur_bytes)
    }
    self.cur_bytes.set(new_cur_bytes);
  }

  pub fn dealloc(&self, num_bytes: usize) {
    assert!(self.cur_bytes.get() >= num_bytes as i64);
    self.cur_bytes.set(self.cur_bytes.get() - num_bytes as i64);
  }
}

// ----------------------------------------------------------------------
// Buffer classes

/// A resize-able byte buffer class.
///
/// Note that a byte buffer has two attributes:
/// `capacity` and `size`: the former is the total bytes allocated for
/// the buffer, while the latter is the actual bytes that have valid data.
/// Invariant: `capacity` >= `size`.
pub struct ByteBuffer {
  data: Vec<u8>,
  mem_tracker: Option<MemTrackerPtr>
}

impl ByteBuffer {
  pub fn new() -> Self {
    ByteBuffer { data: vec!(), mem_tracker: None }
  }

  pub fn with_capacity(mut self, cap: usize) -> Self {
    self.data.reserve(cap);
    self
  }

  pub fn with_mem_tracker(mut self, mc: MemTrackerPtr) -> Self {
    mc.alloc(self.data.capacity());
    self.mem_tracker = Some(mc);
    self
  }

  pub fn data(&self) -> &[u8] {
    self.data.as_slice()
  }

  #[inline]
  pub fn set_data(&mut self, new_data: Vec<u8>) {
    if self.is_mem_tracked() {
      let mc = self.mem_tracker.as_ref().unwrap();
      mc.dealloc(self.data.capacity());
      mc.alloc(new_data.capacity());
    }
    self.data = new_data;
  }

  #[inline]
  pub fn resize(&mut self, new_capacity: usize) {
    let old_capacity = self.data.capacity();
    let extra_capacity = new_capacity - old_capacity;
    if extra_capacity > 0 {
      self.data.reserve(extra_capacity);
      if let Some(ref mc) = self.mem_tracker {
        mc.alloc(self.data.capacity() - old_capacity);
      }
    }
  }

  #[inline]
  pub fn consume(&mut self) -> BytePtr {
    let new_data = vec!();
    if let Some(ref mc) = self.mem_tracker {
      mc.dealloc(self.data.capacity());
      mc.alloc(new_data.capacity());
    }
    let old_data = mem::replace(&mut self.data, new_data);
    BytePtr::new(old_data)
  }

  pub fn capacity(&self) -> usize {
    self.data.capacity()
  }

  pub fn size(&self) -> usize {
    self.data.len()
  }

  #[inline]
  pub fn is_mem_tracked(&self) -> bool {
    self.mem_tracker.is_some()
  }

  pub fn mem_tracker(&self) -> &MemTrackerPtr {
    self.mem_tracker.as_ref().unwrap()
  }
}

impl Write for ByteBuffer {
  fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
    // Check if we have enough capacity for the new data
    if self.data.len() + buf.len() > self.data.capacity() {
      let new_capacity = ::std::cmp::max(
        self.data.capacity() * 2, self.data.len() + buf.len());
      self.resize(new_capacity);
    }
    self.data.write(buf)
  }

  fn flush(&mut self) -> IoResult<()> {
    self.data.flush()
  }
}

impl Drop for ByteBuffer {
  fn drop(&mut self) {
    if self.is_mem_tracked() {
      let mc = self.mem_tracker.as_ref().unwrap();
      mc.dealloc(self.data.capacity());
    }
  }
}


// ----------------------------------------------------------------------
// Immutable Buffer (BytePtr) classes

/// An representation of a slice on a reference-counting and read-only byte array.
/// Sub-slices can be further created from this. The byte array will be released
/// when all slices are dropped.
#[derive(Clone, Debug, PartialEq)]
pub struct BytePtr {
  data: Rc<Vec<u8>>,
  start: usize,
  len: usize
}

impl BytePtr {
  pub fn new(v: Vec<u8>) -> Self {
    let len = v.len();
    Self { data: Rc::new(v), start: 0, len: len }
  }

  pub fn start(&self) -> usize {
    self.start
  }

  pub fn len(&self) -> usize {
    self.len
  }

  pub fn all(&self) -> BytePtr {
    BytePtr { data: self.data.clone(), start: self.start, len: self.len }
  }

  pub fn start_from(&self, start: usize) -> BytePtr {
    assert!(start <= self.len);
    BytePtr { data: self.data.clone(), start: self.start + start, len: self.len - start }
  }

  pub fn range(&self, start: usize, len: usize) -> BytePtr {
    assert!(start + len <= self.len);
    BytePtr { data: self.data.clone(), start: self.start + start, len: len }
  }

  pub fn slice(&self) -> &[u8] {
    &self.data[self.start..self.start + self.len]
  }
}

impl Display for BytePtr {
  fn fmt(&self, f: &mut Formatter) -> FmtResult {
    write!(f, "{:?}", self.data)
  }
}


// ----------------------------------------------------------------------
// MemoryPool classes


/// A central place for managing memory.
/// NOTE: client can only acquire bytes through this API, but not releasing.
/// All the memory will be released once the instance of this trait goes out of scope.
pub struct MemoryPool {
  arena: TypedArena<Vec<u8>>,

  // NOTE: these need to be in `Cell` since all public APIs of
  // this struct take `&self`, instead of `&mut self`. Otherwise, we cannot make the
  // lifetime of outputs to be the same as this memory pool.
  cur_bytes_allocated: Cell<i64>,
  max_bytes_allocated: Cell<i64>
}

impl MemoryPool {
  pub fn new() -> Self {
    let arena = TypedArena::new();
    Self { arena: arena, cur_bytes_allocated: Cell::new(0), max_bytes_allocated: Cell::new(0) }
  }

  /// Acquire a new byte buffer of at least `size` bytes
  /// Return a unique reference to the buffer
  pub fn acquire(&self, size: usize) -> &mut [u8] {
    let buf = vec![0; size];
    self.consume(buf)
  }

  /// Consume `buf` and add it to this memory pool
  /// After the call, `buf` has the same lifetime as the pool.
  /// Return a unique reference to the consumed buffer.
  pub fn consume(&self, data: Vec<u8>) -> &mut [u8] {
    let bytes_allocated = data.capacity();
    let result = self.arena.alloc(data);
    self.cur_bytes_allocated.set(self.cur_bytes_allocated.get() + bytes_allocated as i64);
    self.max_bytes_allocated.set(
      cmp::max(self.max_bytes_allocated.get(), self.cur_bytes_allocated.get()));
    result
  }

  /// Return the total number of bytes allocated so far
  fn cur_allocated(&self) -> i64 {
    self.cur_bytes_allocated.get()
  }

  /// Return the maximum number of bytes allocated so far
  fn max_allocated(&self) -> i64 {
    self.max_bytes_allocated.get()
  }
}


#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_mem_tracker() {
    let result = MemTracker::new_ptr(None);
    assert!(result.is_ok());
    let mem_tracker = result.unwrap();

    let mut buffer = ByteBuffer::new()
      .with_mem_tracker(mem_tracker.clone());
    buffer.set_data(vec![0; 10]);
    assert!(mem_tracker.cur_bytes() >= 10);
    buffer.set_data(vec![0; 20]);
    assert!(mem_tracker.cur_bytes() >= 20);

    {
      let mut buffer2 = ByteBuffer::new()
        .with_capacity(30)
        .with_mem_tracker(mem_tracker.clone());
      assert!(mem_tracker.cur_bytes() >= 50);
      buffer2.set_data(vec![0; 100]);
      assert!(mem_tracker.cur_bytes() >= 120);
    }

    assert!(mem_tracker.cur_bytes() >= 20);
    assert!(mem_tracker.max_bytes() >= 50);

    buffer.resize(40);
    assert!(mem_tracker.cur_bytes() >= 40);

    buffer.consume();
    assert!(mem_tracker.cur_bytes() <= 10);
  }

  #[test]
  fn test_byte_buffer() {
    let mut buffer = ByteBuffer::new();
    assert_eq!(buffer.size(), 0);
    assert_eq!(buffer.capacity(), 0);

    let buffer2 = ByteBuffer::new().with_capacity(40);
    assert_eq!(buffer2.size(), 0);
    assert_eq!(buffer2.capacity(), 40);

    buffer.set_data((0..5).collect());
    assert_eq!(buffer.size(), 5);

    buffer.set_data((0..20).collect());
    assert_eq!(buffer.size(), 20);

    let expected: Vec<u8> = (0..20).collect();
    {
      let data = buffer.data();
      assert_eq!(data, expected.as_slice());
    }

    buffer.resize(40);
    assert!(buffer.capacity() >= 40);

    let byte_ptr = buffer.consume();
    assert_eq!(buffer.size(), 0);
    assert_eq!(byte_ptr.slice(), expected.as_slice());

    let values: Vec<u8> = (0..30).collect();
    let _ = buffer.write(values.as_slice());
    let _ = buffer.flush();

    assert_eq!(buffer.data(), values.as_slice());
  }

  #[test]
  fn test_byte_ptr() {
    let values = (0..50).collect();
    let ptr = BytePtr::new(values);
    assert_eq!(ptr.len(), 50);
    assert_eq!(ptr.start(), 0);

    let ptr2 = ptr.all();
    assert_eq!(ptr2.len(), 50);
    assert_eq!(ptr2.start(), 0);

    let ptr3 = ptr.start_from(20);
    assert_eq!(ptr3.len(), 30);
    assert_eq!(ptr3.start(), 20);

    let ptr4 = ptr3.range(10, 10);
    assert_eq!(ptr4.len(), 10);
    assert_eq!(ptr4.start(), 30);

    let expected: Vec<u8> = (30..40).collect();
    assert_eq!(ptr4.slice(), expected.as_slice());
  }
}
