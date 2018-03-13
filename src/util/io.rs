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
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Read, Result, Seek, SeekFrom};
use std::sync::Mutex;

/// Struct that represents a slice of a file data with independent start position and
/// length. Internally clones provided file handle, wraps with BufReader and resets
/// position before any read.
///
/// This is workaround and alternative for `file.try_clone()` method. It clones `File`
/// while preserving independent position, which is not available with `try_clone()`.
///
/// Designed after `arrow::io::RandomAccessFile`.
pub struct FileChunk {
  reader: Mutex<BufReader<File>>,
  start: usize, // start position in a file
  end: usize // end position in a file
}

impl FileChunk {
  /// Creates new file reader with start and length from a file handle
  pub fn new(fd: &File, start: usize, length: usize) -> Self {
    Self {
      reader: Mutex::new(BufReader::new(fd.try_clone().unwrap())),
      start: start,
      end: start + length
    }
  }
}

impl Read for FileChunk {
  fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
    let mut reader = self.reader.lock()
      .map_err(|err| Error::new(ErrorKind::Other, err.to_string()))?;

    let bytes_to_read = cmp::min(buf.len(), self.end - self.start);
    let buf = &mut buf[0..bytes_to_read];

    reader.seek(SeekFrom::Start(self.start as u64))?;
    let res = reader.read(buf);
    if let Ok(bytes_read) = res {
      self.start += bytes_read;
    }

    res
  }
}


#[cfg(test)]
mod tests {
  use super::*;
  use std::env;

  #[test]
  fn test_io_read_fully() {
    let mut buf = vec![0; 8];
    let mut chunk = FileChunk::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

    let bytes_read = chunk.read(&mut buf[..]).unwrap();
    assert_eq!(bytes_read, 4);
    assert_eq!(buf, vec![b'P', b'A', b'R', b'1', 0, 0, 0, 0]);
  }

  #[test]
  fn test_io_read_in_chunks() {
    let mut buf = vec![0; 4];
    let mut chunk = FileChunk::new(&get_test_file("alltypes_plain.parquet"), 0, 4);

    let bytes_read = chunk.read(&mut buf[0..2]).unwrap();
    assert_eq!(bytes_read, 2);
    let bytes_read = chunk.read(&mut buf[2..]).unwrap();
    assert_eq!(bytes_read, 2);
    assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
  }

  #[test]
  fn test_io_seek_switch() {
    let mut buf = vec![0; 4];
    let mut file = get_test_file("alltypes_plain.parquet");
    let mut chunk = FileChunk::new(&file, 0, 4);

    file.seek(SeekFrom::Start(5 as u64)).expect("File seek to a position");

    let bytes_read = chunk.read(&mut buf[..]).unwrap();
    assert_eq!(bytes_read, 4);
    assert_eq!(buf, vec![b'P', b'A', b'R', b'1']);
  }

  fn get_test_file(file_name: &str) -> File {
    let mut path_buf = env::current_dir().unwrap();
    path_buf.push("data");
    path_buf.push(file_name);
    let file = File::open(path_buf.as_path());
    assert!(file.is_ok());
    file.unwrap()
  }
}
