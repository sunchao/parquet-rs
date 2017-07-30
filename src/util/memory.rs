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
use std::fmt::{Display, Result as FmtResult, Formatter, Debug};
use std::io::{Result as IoResult, Write};
use std::mem;
use std::ops::{Index, IndexMut};
use std::rc::{Rc, Weak};

use arena::TypedArena;
use errors::Result;


// ----------------------------------------------------------------------
// Memory Tracker classes

pub type MemTrackerPtr = Rc<MemTracker>;
pub type WeakMemTrackerPtr = Weak<MemTracker>;

#[derive(Debug)]
pub struct MemTracker {
  cur_bytes: Cell<i64>,
  max_bytes: Cell<i64>,
  children: RefCell<Vec<MemTrackerPtr>>,
  parent: Option<WeakMemTrackerPtr>
}

impl MemTracker {
  #[inline]
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

  #[inline]
  pub fn alloc(&self, num_bytes: i64) {
    let new_cur_bytes = self.cur_bytes.get() + num_bytes;
    if new_cur_bytes > self.max_bytes.get() {
      self.max_bytes.set(new_cur_bytes)
    }
    self.cur_bytes.set(new_cur_bytes);
  }
}


// ----------------------------------------------------------------------
// Buffer classes

pub type ByteBuffer = Buffer<u8>;
pub type ByteBufferPtr = BufferPtr<u8>;

/// A resize-able buffer class with generic member, with optional memory tracker.
///
/// Note that a buffer has two attributes:
/// `capacity` and `size`: the former is the total number of space reserved for
/// the buffer, while the latter is the actual number of elements.
/// Invariant: `capacity` >= `size`.
/// The total allocated bytes for a buffer equals to `capacity * sizeof<T>()`.
///
pub struct Buffer<T: Clone> {
  data: Vec<T>,
  mem_tracker: Option<MemTrackerPtr>,
  type_length: usize
}

impl<T: Clone> Buffer<T> {
  pub fn new() -> Self {
    Buffer { data: vec!(), mem_tracker: None, type_length: ::std::mem::size_of::<T>() }
  }

  #[inline]
  pub fn with_mem_tracker(mut self, mc: MemTrackerPtr) -> Self {
    mc.alloc((self.data.capacity() * self.type_length) as i64);
    self.mem_tracker = Some(mc);
    self
  }

  #[inline]
  pub fn data(&self) -> &[T] {
    self.data.as_slice()
  }

  #[inline]
  pub fn set_data(&mut self, new_data: Vec<T>) {
    if let Some(ref mc) = self.mem_tracker {
      let capacity_diff = new_data.capacity() as i64 - self.data.capacity() as i64;
      mc.alloc(capacity_diff * self.type_length as i64);
    }
    self.data = new_data;
  }

  #[inline]
  pub fn resize(&mut self, new_size: usize, init_value: T) {
    let old_capacity = self.data.capacity();
    self.data.resize(new_size, init_value);
    if let Some(ref mc) = self.mem_tracker {
      let capacity_diff = self.data.capacity() as i64 - old_capacity as i64;
      mc.alloc(capacity_diff * self.type_length as i64);
    }
  }

  #[inline]
  pub fn clear(&mut self) {
    self.data.clear()
  }

  #[inline]
  pub fn reserve(&mut self, additional_capacity: usize) {
    let old_capacity = self.data.capacity();
    self.data.reserve(additional_capacity);
    if self.data.capacity() > old_capacity {
      if let Some(ref mc) = self.mem_tracker {
        let capacity_diff = self.data.capacity() as i64 - old_capacity as i64;
        mc.alloc(capacity_diff * self.type_length as i64);
      }
    }
  }

  #[inline]
  pub fn consume(&mut self) -> BufferPtr<T> {
    let old_data = mem::replace(&mut self.data, vec!());
    let mut result = BufferPtr::new(old_data);
    if let Some(ref mc) = self.mem_tracker {
      result = result.with_mem_tracker(mc.clone());
    }
    result
  }

  #[inline]
  pub fn push(&mut self, value: T) {
    self.data.push(value)
  }

  #[inline]
  pub fn capacity(&self) -> usize {
    self.data.capacity()
  }

  #[inline]
  pub fn size(&self) -> usize {
    self.data.len()
  }

  #[inline]
  pub fn is_mem_tracked(&self) -> bool {
    self.mem_tracker.is_some()
  }

  #[inline]
  pub fn mem_tracker(&self) -> &MemTrackerPtr {
   self.mem_tracker.as_ref().unwrap()
  }
}

impl<T: Sized + Clone> Index<usize> for Buffer<T> {
  type Output = T;
  fn index(&self, index: usize) -> &T {
    &self.data[index]
  }
}

impl<T: Sized + Clone> IndexMut<usize> for Buffer<T> {
  fn index_mut(&mut self, index: usize) -> &mut T {
    &mut self.data[index]
  }
}

// TODO: implement this for other types
impl Write for Buffer<u8> {
  #[inline]
  fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
    let old_capacity = self.data.capacity();
    let bytes_written = self.data.write(buf)?;
    if let Some(ref mc) = self.mem_tracker {
      if self.data.capacity() - old_capacity > 0 {
        mc.alloc((self.data.capacity() - old_capacity) as i64)
      }
    }
    Ok(bytes_written)
  }

  fn flush(&mut self) -> IoResult<()> {
    // No-op
    self.data.flush()
  }
}

impl AsRef<[u8]> for Buffer<u8> {
  fn as_ref(&self) -> &[u8] {
    self.data.as_slice()
  }
}

impl<T: Clone> Drop for Buffer<T> {
  #[inline]
  fn drop(&mut self) {
    if let Some(ref mc) = self.mem_tracker {
      mc.alloc(-((self.data.capacity() * self.type_length) as i64));
    }
  }
}


// ----------------------------------------------------------------------
// Immutable Buffer (BufferPtr) classes

/// An representation of a slice on a reference-counting and read-only byte array.
/// Sub-slices can be further created from this. The byte array will be released
/// when all slices are dropped.
#[derive(Clone, Debug)]
pub struct BufferPtr<T> {
  data: Rc<Vec<T>>,
  start: usize,
  len: usize,
  // TODO: will this create too many references? rethink about this.
  mem_tracker: Option<MemTrackerPtr>
}

impl<T> BufferPtr<T> {
  pub fn new(v: Vec<T>) -> Self {
    let len = v.len();
    Self { data: Rc::new(v), start: 0, len: len, mem_tracker: None }
  }

  pub fn data(&self) -> &[T] {
    &self.data[self.start..self.start + self.len]
  }

  pub fn with_range(mut self, start: usize, len: usize) -> Self {
    assert!(start <= self.len);
    assert!(start + len <= self.len);
    self.start = start;
    self.len = len;
    self
  }

  pub fn with_mem_tracker(mut self, mc: MemTrackerPtr) -> Self {
    self.mem_tracker = Some(mc);
    self
  }

  pub fn start(&self) -> usize {
    self.start
  }

  pub fn len(&self) -> usize {
    self.len
  }

  pub fn is_mem_tracked(&self) -> bool {
    self.mem_tracker.is_some()
  }

  pub fn all(&self) -> BufferPtr<T> {
    BufferPtr {
      data: self.data.clone(), start: self.start,
      len: self.len, mem_tracker: self.mem_tracker.as_ref().map(|p| p.clone())
    }
  }

  pub fn start_from(&self, start: usize) -> BufferPtr<T> {
    assert!(start <= self.len);
    BufferPtr {
      data: self.data.clone(), start: self.start + start,
      len: self.len - start, mem_tracker: self.mem_tracker.as_ref().map(|p| p.clone())
    }
  }

  pub fn range(&self, start: usize, len: usize) -> BufferPtr<T> {
    assert!(start + len <= self.len);
    BufferPtr {
      data: self.data.clone(), start: self.start + start,
      len: len, mem_tracker: self.mem_tracker.as_ref().map(|p| p.clone())
    }
  }
}

impl<T: Sized> Index<usize> for BufferPtr<T> {
  type Output = T;
  fn index(&self, index: usize) -> &T {
    assert!(index < self.len);
    &self.data[self.start + index]
  }
}

impl<T: Debug> Display for BufferPtr<T> {
  fn fmt(&self, f: &mut Formatter) -> FmtResult {
    write!(f, "{:?}", self.data)
  }
}

impl<T> Drop for BufferPtr<T> {
  fn drop(&mut self) {
    if self.is_mem_tracked() &&
      Rc::strong_count(&self.data) == 1 && Rc::weak_count(&self.data) == 0 {
      let mc = self.mem_tracker.as_ref().unwrap();
      mc.alloc(-(self.data.capacity() as i64));
    }
  }
}

impl AsRef<[u8]> for BufferPtr<u8> {
  fn as_ref(&self) -> &[u8] {
    &self.data[self.start..self.start + self.len]
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
  fn test_byte_buffer_mem_tracker() {
    let result = MemTracker::new_ptr(None);
    assert!(result.is_ok());
    let mem_tracker = result.unwrap();

    let mut buffer = ByteBuffer::new()
      .with_mem_tracker(mem_tracker.clone());
    buffer.set_data(vec![0; 10]);
    assert_eq!(mem_tracker.cur_bytes(), buffer.capacity() as i64);
    buffer.set_data(vec![0; 20]);
    let capacity = buffer.capacity() as i64;
    assert_eq!(mem_tracker.cur_bytes(), capacity);

    let max_capacity =
    {
      let mut buffer2 = ByteBuffer::new()
        .with_mem_tracker(mem_tracker.clone());
      buffer2.reserve(30);
      assert_eq!(mem_tracker.cur_bytes(), buffer2.capacity() as i64 + capacity);
      buffer2.set_data(vec![0; 100]);
      assert_eq!(mem_tracker.cur_bytes(), buffer2.capacity() as i64 + capacity);
      buffer2.capacity() as i64 + capacity
    };

    assert_eq!(mem_tracker.cur_bytes(), capacity);
    assert_eq!(mem_tracker.max_bytes(), max_capacity);

    buffer.reserve(40);
    assert_eq!(mem_tracker.cur_bytes(), buffer.capacity() as i64);

    buffer.consume();
    assert_eq!(mem_tracker.cur_bytes(), buffer.capacity() as i64);
  }

  #[test]
  fn test_byte_ptr_mem_tracker() {
    let result = MemTracker::new_ptr(None);
    assert!(result.is_ok());
    let mem_tracker = result.unwrap();

    let mut buffer = ByteBuffer::new()
      .with_mem_tracker(mem_tracker.clone());
    buffer.set_data(vec![0; 60]);

    {
      let buffer_capacity = buffer.capacity() as i64;
      let buf_ptr = buffer.consume();
      assert_eq!(mem_tracker.cur_bytes(), buffer_capacity);
      {
        let buf_ptr1 = buf_ptr.all();
        {
          let _ = buf_ptr.start_from(20);
          assert_eq!(mem_tracker.cur_bytes(), buffer_capacity);
        }
        assert_eq!(mem_tracker.cur_bytes(), buffer_capacity);
        let _ = buf_ptr1.range(30, 20);
        assert_eq!(mem_tracker.cur_bytes(), buffer_capacity);
      }
      assert_eq!(mem_tracker.cur_bytes(), buffer_capacity);
    }
    assert_eq!(mem_tracker.cur_bytes(), buffer.capacity() as i64);
  }

  #[test]
  fn test_byte_buffer() {
    let mut buffer = ByteBuffer::new();
    assert_eq!(buffer.size(), 0);
    assert_eq!(buffer.capacity(), 0);

    let mut buffer2 = ByteBuffer::new();
    buffer2.reserve(40);
    assert_eq!(buffer2.size(), 0);
    assert_eq!(buffer2.capacity(), 40);

    buffer.set_data((0..5).collect());
    assert_eq!(buffer.size(), 5);
    assert_eq!(buffer[4], 4);

    buffer.set_data((0..20).collect());
    assert_eq!(buffer.size(), 20);
    assert_eq!(buffer[10], 10);

    let expected: Vec<u8> = (0..20).collect();
    {
      let data = buffer.data();
      assert_eq!(data, expected.as_slice());
    }

    buffer.reserve(40);
    assert!(buffer.capacity() >= 40);

    let byte_ptr = buffer.consume();
    assert_eq!(buffer.size(), 0);
    assert_eq!(byte_ptr.as_ref(), expected.as_slice());

    let values: Vec<u8> = (0..30).collect();
    let _ = buffer.write(values.as_slice());
    let _ = buffer.flush();

    assert_eq!(buffer.data(), values.as_slice());
  }

  #[test]
  fn test_byte_ptr() {
    let values = (0..50).collect();
    let ptr = ByteBufferPtr::new(values);
    assert_eq!(ptr.len(), 50);
    assert_eq!(ptr.start(), 0);
    assert_eq!(ptr[40], 40);

    let ptr2 = ptr.all();
    assert_eq!(ptr2.len(), 50);
    assert_eq!(ptr2.start(), 0);
    assert_eq!(ptr2[40], 40);

    let ptr3 = ptr.start_from(20);
    assert_eq!(ptr3.len(), 30);
    assert_eq!(ptr3.start(), 20);
    assert_eq!(ptr3[0], 20);

    let ptr4 = ptr3.range(10, 10);
    assert_eq!(ptr4.len(), 10);
    assert_eq!(ptr4.start(), 30);
    assert_eq!(ptr4[0], 30);

    let expected: Vec<u8> = (30..40).collect();
    assert_eq!(ptr4.as_ref(), expected.as_slice());
  }
}
