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

use std::{cmp, fmt, mem, ptr, slice};
use std::cell::{Cell, RefCell};
use std::io::{Result as IoResult, Error as IoError, ErrorKind, Write};
use std::ops::{Index, IndexMut, Range, RangeFrom, RangeTo, RangeFull};
use std::rc::{Rc, Weak};

use super::bit_util;

// ----------------------------------------------------------------------
// Memory Tracker classes

pub type MemTrackerPtr = Rc<MemTracker>;
pub type WeakMemTrackerPtr = Weak<MemTracker>;

#[derive(Debug)]
pub struct MemTracker {
  // Memory usage information tracked by this. In the tuple, the first element is the
  // current memory allocated (in bytes), and the second element is the maximum memory
  // allocated so far (in bytes).
  memory_usage: Cell<(i64, i64)>
}

impl MemTracker {
  #[inline]
  pub fn new() -> MemTracker {
    MemTracker {
      memory_usage: Cell::new((0, 0))
    }
  }

  /// Returns the current memory consumption, in bytes.
  pub fn memory_usage(&self) -> i64 {
    self.memory_usage.get().0
  }

  /// Returns the maximum memory consumption so far, in bytes.
  pub fn max_memory_usage(&self) -> i64 {
    self.memory_usage.get().1
  }

  /// Adds `num_bytes` to the memory consumption tracked by this memory tracker.
  #[inline]
  pub fn alloc(&self, num_bytes: i64) {
    let (current, mut maximum) = self.memory_usage.get();
    let new_current = current + num_bytes;
    if new_current > maximum { maximum = new_current }
    self.memory_usage.set((new_current, maximum));
  }
}


// ----------------------------------------------------------------------
// Buffer classes

/// An non-resizable and mutable buffer that is merely a pointer to a memory region owned
/// by some other structure (e.g., arena). Once the owner goes out of scope, it is illegal
/// to use the buffer instances - undefined behavior will occur if that happens.
/// TODO: we could use reference counting to track any dangling buffers, and throw error
/// in the illegal case.
#[derive(Clone, Debug)]
pub struct Buffer {
  // The pointer to the data buffer
  ptr: *mut u8,
  // The length of the data buffer
  len: usize,
  // Whether the data buffer is owned by this struct. Should be dropped if so.
  is_owner: bool
}

impl Buffer {
  pub fn new() -> Self {
    Self { ptr: ptr::null_mut(), len: 0, is_owner: false }
  }

  pub fn from_ptr(ptr: *mut u8, len: usize) -> Self {
    Self { ptr: ptr, len: len, is_owner: false }
  }

  pub fn len(&self) -> usize {
    self.len
  }

  pub fn raw_data(&self) -> *const u8 {
    self.ptr
  }

  pub fn raw_data_mut(&self) -> *mut u8 {
    self.ptr
  }

  pub fn data(&self) -> &[u8] {
    unsafe {
      slice::from_raw_parts(self.ptr, self.len)
    }
  }

  pub fn data_mut(&self) -> &mut [u8] {
    unsafe {
      slice::from_raw_parts_mut(self.ptr, self.len)
    }
  }
}

impl Drop for Buffer {
  fn drop(&mut self) {
    if self.is_owner {
      let _ = unsafe {
        Vec::from_raw_parts(self.ptr, self.len, self.len)
      };
    }
  }
}

/// Convert a byte slice to a buffer.
/// NOTE: THIS IS VERY UNSAFE!
/// The caller has to guarantee the vector for the slice won't go out of scope before it
/// is done using this buffer. Otherwise, undefined behavior could happen. Ideally we
/// should only use this in tests.
// impl<'a> From<&'a mut [u8]> for Buffer {
//   fn from(s: &'a mut [u8]) -> Buffer {
//     Buffer {
//       ptr: s.as_mut_ptr(),
//       len: s.len()
//     }
//   }
// }

impl<'a> From<Vec<u8>> for Buffer {
  fn from(v: Vec<u8>) -> Buffer {
    let len = v.len();
    let ptr = Box::into_raw(v.into_boxed_slice()) as *mut [u8] as *mut u8;
    Buffer {
      ptr: ptr,
      len: len,
      is_owner: true
    }
  }
}

impl<'a> From<String> for Buffer {
  fn from(s: String) -> Buffer {
    let len = s.len();
    let ptr = Box::into_raw(s.into_bytes().into_boxed_slice()) as *mut [u8] as *mut u8;
    Buffer {
      ptr: ptr,
      len: len,
      is_owner: true
    }
  }
}

impl Index<usize> for Buffer {
  type Output = u8;
  fn index(&self, index: usize) -> &u8 {
    assert!(index < self.len, "Index {} out of bound", index);
    unsafe {
      &*self.ptr.offset(index as isize)
    }
  }
}

impl IndexMut<usize> for Buffer {
  fn index_mut(&mut self, index: usize) -> &mut u8 {
    assert!(index < self.len, "Index {} out of bound", index);
    unsafe {
      &mut *self.ptr.offset(index as isize)
    }
  }
}

impl Write for Buffer {
  fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
    if self.len < buf.len() {
      return Err(IoError::new(ErrorKind::Other, "Input buffer too large"));
    }
    let dst = self.data_mut();
    bit_util::memcpy(buf, dst);
    Ok(buf.len())
  }

  fn flush(&mut self) -> IoResult<()> {
    // No-op
    Ok(())
  }
}

// For debugging
impl fmt::Binary for Buffer {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    write!(f, "{:?}[len={}][", self.ptr, self.len)?;
    let mut i = 0;
    for b in self.data() {
      write!(f, "({}){:b}", i, b)?;
      i += 1;
    }
    write!(f, "]")?;
    Ok(())
  }
}

/// Similar to `std::ops::Index`, but returns value instead of ref.
pub trait BufferRange<RangeType> {
  fn range(&self, range: RangeType) -> Buffer;
}

impl BufferRange<RangeFrom<usize>> for Buffer {
  fn range(&self, range: RangeFrom<usize>) -> Buffer {
    if range.start > self.len {
      panic!("Index out of bound: start {} > len {}", range.start, self.len)
    }
    Buffer {
      ptr: unsafe { self.ptr.offset(range.start as isize) },
      len: self.len - range.start,
      is_owner: false // TODO: the range buffer cannot live longer than original.
    }
  }
}

impl BufferRange<RangeTo<usize>> for Buffer {
  fn range(&self, range: RangeTo<usize>) -> Buffer {
    if range.end > self.len {
      panic!("Index out of bound: end {} > len {}", range.end, self.len)
    }
    Buffer {
      ptr: self.ptr,
      len: range.end,
      is_owner: false
    }
  }
}

impl BufferRange<RangeFull> for Buffer {
  fn range(&self, _: RangeFull) -> Buffer {
    Buffer {
      ptr: self.ptr,
      len: self.len,
      is_owner: false
    }
  }
}

impl BufferRange<Range<usize>> for Buffer {
  fn range(&self, range: Range<usize>) -> Buffer {
    if range.start > self.len {
      panic!("Index out of bound: start {} > len {}", range.start, self.len)
    }
    if range.end > self.len {
      panic!("Index out of bound: end ({}) > len ({})", range.end, self.len)
    }
    Buffer {
      ptr: unsafe { self.ptr.offset(range.start as isize) },
      len: range.end - range.start,
      is_owner: false
    }
  }

}


/// A resize-able buffer whose memory usage is tracked.
pub struct ResizableBuffer {
  data: Vec<u8>,
  arena: ArenaRef
}

impl ResizableBuffer {
  pub fn new(init_capacity: usize, arena: ArenaRef) -> Self {
    Self {
      data: Vec::with_capacity(init_capacity),
      arena: arena
    }
  }

  #[inline]
  pub fn len(&self) -> usize {
    self.data.len()
  }

  #[inline]
  pub fn data(&mut self) -> &[u8] {
    &self.data[..]
  }

  #[inline]
  pub fn data_mut(&mut self) -> &mut [u8] {
    &mut self.data[..]
  }

  #[inline]
  pub fn vec(&mut self) -> &mut Vec<u8> {
    &mut self.data
  }

  pub fn extend(&mut self, buf: &[u8]) {
    let data_len = self.data.len();
    if self.data.capacity() - data_len < buf.len() {
      self.resize(data_len + buf.len());
    }
    self.data.extend_from_slice(buf);
  }

  pub fn resize(&mut self, new_capacity: usize) -> bool {
    let old_capacity = self.data.capacity();
    if old_capacity >= new_capacity {
      return false
    }
    let cap = cmp::max(new_capacity, old_capacity * 2);
    let additional = cap - old_capacity;
    self.data.reserve_exact(additional);
    self.arena.mem_tracker().alloc(additional as i64);
    return true;
  }

  /// Turns this buffer into a non-resizable `Buffer`.
  pub fn to_fixed(self) -> Buffer {
    let mut blocks = self.arena.blocks.borrow_mut();
    let mut b: Box<[u8]> = self.data.into_boxed_slice();
    let ptr = b.as_mut_ptr();
    let len = b.len();
    blocks.push(b);
    Buffer { ptr: ptr, len: len, is_owner: false }
  }
}


impl Write for ResizableBuffer {
  fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
    // Check if we have enough capacity to hold `buf`.
    let data_len = self.data.len();
    if self.data.capacity() - data_len < buf.len() {
      self.resize(data_len + buf.len());
    }
    self.data.write(buf)
  }

  fn flush(&mut self) -> IoResult<()> {
    // No-op
    Ok(())
  }
}

const BLOCK_SIZE : usize = 4096;

pub type ArenaRef = Rc<Arena>;

pub struct Arena {
  cur_block: RefCell<Block>,
  mem_tracker: MemTrackerPtr,
  blocks: RefCell<Vec<Box<[u8]>>>
}

struct Block {
  ptr: *mut u8,
  bytes_remaining: usize
}

impl Arena {
  pub fn new() -> Self {
    Arena::with_mem_tracker(Rc::new(MemTracker::new()))
  }

  pub fn with_mem_tracker(mem_tracker: MemTrackerPtr) -> Self {
    let b = Block { ptr: ptr::null_mut(), bytes_remaining: 0 };
    Self {
      cur_block: RefCell::new(b),
      mem_tracker: mem_tracker,
      blocks: RefCell::new(Vec::new())
    }
  }

  pub fn alloc(&self, bytes: usize) -> Buffer {
    let need_alloc = {
      let block = self.cur_block.borrow_mut();
      bytes > block.bytes_remaining
    };
    if need_alloc {
      self.alloc_fallback(bytes)
    } else {
      let mut block = self.cur_block.borrow_mut();
      assert!(!block.ptr.is_null(), "ptr should NOT be null");
      let res_ptr = block.ptr;
      unsafe {
        block.ptr = block.ptr.offset(bytes as isize);
        block.bytes_remaining -= bytes;
        Buffer { ptr: res_ptr, len: bytes, is_owner: false }
      }
    }
  }

  fn alloc_fallback(&self, bytes: usize) -> Buffer {
    if bytes > BLOCK_SIZE / 4 {
      let new_block = self.alloc_new(bytes);
      return Buffer {
        ptr: new_block.ptr,
        len: new_block.bytes_remaining,
        is_owner: false
      }
    }

    let mut block = self.alloc_new(BLOCK_SIZE);
    let res_ptr = block.ptr;
    unsafe {
      block.ptr = block.ptr.offset(bytes as isize);
      block.bytes_remaining -= bytes;
      self.cur_block.replace(block);
      Buffer { ptr: res_ptr, len: bytes, is_owner: false }
    }
  }

  fn alloc_new(&self, bytes: usize) -> Block {
    unsafe {
      let mut v = Vec::with_capacity(bytes);
      v.set_len(bytes);
      let mut b: Box<[u8]> = v.into_boxed_slice();
      let ptr = b.as_mut_ptr();
      ptr::write_bytes(ptr, 0, bytes);
      let mut blocks = self.blocks.borrow_mut();
      blocks.push(b);
      self.mem_tracker.alloc(bytes as i64);
      Block { ptr: ptr, bytes_remaining: bytes }
    }
  }

  pub fn alloc_aligned(&self, bytes: usize) -> Buffer {
    let ptr_size = mem::size_of::<usize>();
    assert!(ptr_size <= 128);
    let align = if ptr_size > 8 { ptr_size } else { 8 };
    assert!(align & (align - 1) == 0);

    let (ptr, bytes_remaining) = {
      let block = self.cur_block.borrow();
      (block.ptr, block.bytes_remaining)
    };
    let current_mod = ptr as usize & (align - 1);
    let slop = if current_mod == 0 { 0 } else { align - current_mod };
    let needed = bytes + slop;
    let result = if needed <= bytes_remaining {
      unsafe {
        let mut block = self.cur_block.borrow_mut();
        let p = block.ptr.offset(slop as isize);
        block.ptr = block.ptr.offset(needed as isize);
        block.bytes_remaining -= needed;
        Buffer { ptr: p, len: bytes, is_owner: false }
      }
    } else {
      // `alloc_fallback()` always allocate aligned memory
      self.alloc_fallback(bytes)
    };
    assert!(result.ptr as usize & (align - 1) == 0);
    result
  }

  pub fn alloc_resizable(arena: &ArenaRef, bytes: usize) -> ResizableBuffer {
    arena.mem_tracker().alloc(bytes as i64);
    ResizableBuffer {
      data: Vec::with_capacity(bytes),
      arena: arena.clone()
    }
  }

  pub fn mem_tracker(&self) -> MemTrackerPtr {
    self.mem_tracker.clone()
  }

  pub fn memory_usage(&self) -> i64 {
    self.mem_tracker.memory_usage()
  }

  pub fn max_memory_usage(&self) -> i64 {
    self.mem_tracker.max_memory_usage()
  }
}


#[cfg(test)]
mod tests {
    use super::*;

  // ----------------------------------------------------------------------
  // Tests for MemTracker
  #[test]
  fn test_mem_tracker() {
    let mc = MemTracker::new();
    mc.alloc(100);
    assert_eq!(mc.memory_usage(), 100);
    assert_eq!(mc.max_memory_usage(), 100);

    mc.alloc(50);
    assert_eq!(mc.memory_usage(), 150);
    assert_eq!(mc.max_memory_usage(), 150);

    mc.alloc(-50);
    assert_eq!(mc.memory_usage(), 100);
    assert_eq!(mc.max_memory_usage(), 150);
  }

  // ----------------------------------------------------------------------
  // Tests for Arena

  #[test]
  fn test_new() {
    let arena = Arena::new();
    check_current_block(&arena, true, 0);
    assert_eq!(arena.memory_usage(), 0);
  }

  #[test]
  fn test_alloc_new() {
    let arena = Arena::new();

    let _ = arena.alloc_new(128);
    check_current_block(&arena, true, 0);
    assert_eq!(arena.memory_usage(), 128);

    let _ = arena.alloc_new(256);
    check_current_block(&arena, true, 0);
    assert_eq!(arena.memory_usage(), 256 + 128);
  }

  #[test]
  fn test_alloc_fallback() {
    let arena = Arena::new();

    let _ = arena.alloc_fallback(1025);
    check_current_block(&arena, true, 0);
    assert_eq!(arena.memory_usage(), 1025);

    let _ = arena.alloc_fallback(512);
    check_current_block(&arena, false, BLOCK_SIZE - 512);
    assert_eq!(arena.memory_usage(), 1025 + BLOCK_SIZE as i64);
  }

  #[test]
  fn test_alloc_aligned() {
    let arena = Arena::new();
    let ptr_size = ::std::mem::size_of::<usize>();
    assert!(ptr_size > 1);

    let _ = arena.alloc_fallback(1);
    check_current_block(&arena, false, BLOCK_SIZE - 1);

    let _ = arena.alloc_aligned(3000);
    check_current_block(&arena, false, BLOCK_SIZE - 3000 - ptr_size);

    let _ = arena.alloc_aligned(1000);
    check_current_block(&arena, false, BLOCK_SIZE - 3000 - 1000 - ptr_size);

    // should allocate a new block
    let _ = arena.alloc_aligned(500);
    check_current_block(&arena, false, BLOCK_SIZE - 500);
  }

  #[test]
  fn test_alloc() {
    let arena = Arena::new();

    let _ = arena.alloc(128);

    check_current_block(&arena, false, 3968); // 4096 - 128
    assert_eq!(arena.memory_usage(), 4096);

    let _ = arena.alloc(1024); // should allocate from existing block

    check_current_block(&arena, false, 2944); // 3968 - 1024
    assert_eq!(arena.memory_usage(), 4096);

    let _ = arena.alloc(8192); // should allocate new block

    check_current_block(&arena, false, 2944);
    assert_eq!(arena.memory_usage(), 12288); // 8192 + 4096

    let _ = arena.alloc(2048); // should allocate from existing block
    check_current_block(&arena, false, 896); // 2944 - 2048
    assert_eq!(arena.memory_usage(), 12288);

    let _ = arena.alloc(1024); // should allocate new block

    check_current_block(&arena, false, 3072); // 4096 - 1024
    assert_eq!(arena.memory_usage(), 16384); // 12288 + 4096
  }

    fn check_current_block(arena: &Arena, is_null: bool, bytes: usize) {
      let block = arena.cur_block.borrow();
      assert_eq!(block.ptr.is_null(), is_null);
      assert_eq!(block.bytes_remaining, bytes);
    }
}
