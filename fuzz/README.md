# Fuzzing

To run the fuzzer you can follow the instructions on: https://fuzz.rs/book/afl.html

## Run fuzzing

To install AFL you can run `cargo install afl --force`. Due to some compatibility issues, on OSX, you should install a specific version (`cargo install afl --force --version 0.4.1`) until https://github.com/rust-fuzz/afl.rs/issues/146 gets resolved.

Once you have AFL installed, you can start fuzzing with:

```bash
cargo afl build
cargo afl fuzz -i in -o out target/debug/parquet-fuzz
```

After founding some crashes or hangs, you can find the outputs on the `out/crashes` or `out/hangs` folder.
Then, you can run this command on the root of the project to check the trace of the crash:


```bash
RUST_BACKTRACE=1 cargo run --bin parquet-read parquet-fuzz/out/crashes/{filename}
```