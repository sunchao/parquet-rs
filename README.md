# parquet-rs

[![Build Status](https://travis-ci.org/sunchao/parquet-rs.svg?branch=master)](https://travis-ci.org/sunchao/parquet-rs)
[![Coverage Status](https://coveralls.io/repos/github/sunchao/parquet-rs/badge.svg?branch=master)](https://coveralls.io/github/sunchao/parquet-rs?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

An [Apache Parquet](https://parquet.apache.org/) implementation in Rust (work in progress)

## Development
### Requirements
- Rust nightly
- Thrift 0.11+ or master (with Rust support)

To install Rust nightly build, just run (assuming you have Rust installed already):
```shell
rustup update nightly
rustup default nightly
```

Project requires Thrift with Rust support, which is in 0.11+/master. If release is not available on
official website, follow instructions:
```shell
git clone --depth=1 https://github.com/apache/thrift
# go to thrift project directory and install it
cd thrift

# export env vars for openssl
LD_LIBRARY_PATH=/usr/local/opt/openssl/lib:"${LD_LIBRARY_PATH}"
CPATH=/usr/local/opt/openssl/include:"${CPATH}"
PKG_CONFIG_PATH=/usr/local/opt/openssl/lib/pkgconfig:"${PKG_CONFIG_PATH}"
export LD_LIBRARY_PATH CPATH PKG_CONFIG_PATH

./bootstrap.sh
# when configuring you might want to keep set of languages to a minimum, e.g. C++/Java/Rust
./configure

make install
```

On OS X you might need to pre-install following packages (if not installed already):
```shell
brew install openssl libtool automake pkg-config
```

### Build and test
To build project run:
```shell
cd parquet-rs
make

# clean generated files
make clean

# run tests
make test

# install 'rust-clippy' for this ('cargo install clippy')
make clippy
```
