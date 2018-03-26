# parquet-rs

[![Build Status](https://travis-ci.org/sunchao/parquet-rs.svg?branch=master)](https://travis-ci.org/sunchao/parquet-rs)
[![Coverage Status](https://coveralls.io/repos/github/sunchao/parquet-rs/badge.svg?branch=master)](https://coveralls.io/github/sunchao/parquet-rs?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

An [Apache Parquet](https://parquet.apache.org/) implementation in Rust (work in progress)

## Requirements
- Rust nightly
- Thrift 0.11.0 or higher

See [Working with nightly Rust](https://github.com/rust-lang-nursery/rustup.rs/blob/master/README.md#working-with-nightly-rust)
to install nightly toolchain and set it as default. Follow instructions on [Apache Thrift](https://thrift.apache.org)
website to install the required version of Thrift (it may be necessary to build from source).

For more information on setup and build refer to [.travis.yml](./.travis.yml#L15) file
(`before_script` section).

## System Dependencies
All of the dependencies are required by Thrift install, please see [Apache Thrift](https://thrift.apache.org)
website for installing or updating necessary dependencies.

## Build
Run `cargo build` or `cargo build --release` to build in release mode.

## Test
Run `cargo test` for unit tests.

## Benchmarks
Run `cargo bench` for benchmarks.

## License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0.
