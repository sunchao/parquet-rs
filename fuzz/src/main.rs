extern crate afl;
extern crate parquet;

use std::io::Cursor;
use afl::fuzz;
use parquet::file::reader::FileReader;
use parquet::errors::Result;

/// Hack to circumvent that SerializedFileReader only allows static inputs
unsafe fn extend_lifetime<'b>(r: &'b [u8]) -> &'static[u8] {
    std::mem::transmute::<&'b [u8], &'static [u8]>(r)
}

fn main() {
    fuzz(|string| {
        let static_input = unsafe { extend_lifetime(string.as_ref()) };

        let _ = fuzz_input(static_input);
    });
}

fn fuzz_input(input: &'static [u8]) -> Result<()> {
    let cursor = Cursor::new(input);
    let file_reader = parquet::file::reader::SerializedFileReader::new(cursor)?;
    let iter  = file_reader.get_row_iter(None)?;

    for _ in iter {
        ;
    }

    Ok(())
}
