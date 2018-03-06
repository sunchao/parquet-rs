# parquet-rs

[![Build Status](https://travis-ci.org/sunchao/parquet-rs.svg?branch=master)](https://travis-ci.org/sunchao/parquet-rs)
[![Coverage Status](https://coveralls.io/repos/github/sunchao/parquet-rs/badge.svg?branch=master)](https://coveralls.io/github/sunchao/parquet-rs?branch=master)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

An [Apache Parquet](https://parquet.apache.org/) implementation in Rust (work in progress)

## Development
### Requirements
- Rust nightly
- Thrift 0.11.0 or higher

To install Rust nightly build, run this (assuming you have Rust installed already):
```shell
rustup update nightly
rustup default nightly
```

Building project requires Thrift 0.11.0 or higher, which comes with the Rust
support. To build from source:
```shell
git clone --depth=1 -b 0.11.0 https://github.com/apache/thrift
cd thrift

# export env vars for openssl (this might not be needed depending on packages installation)
LD_LIBRARY_PATH=/usr/local/opt/openssl/lib:"${LD_LIBRARY_PATH}"
CPATH=/usr/local/opt/openssl/include:"${CPATH}"
PKG_CONFIG_PATH=/usr/local/opt/openssl/lib/pkgconfig:"${PKG_CONFIG_PATH}"
export LD_LIBRARY_PATH CPATH PKG_CONFIG_PATH

./bootstrap.sh

# no need to include 'with-rs' flag, 'cargo build' will build this dependency
./configure --enable-libs=no

make install
```

On OS X you might need to pre-install following packages (if not installed already):
```shell
brew install boost libevent openssl libtool automake pkg-config
```

For more information on setup and build refer to [.travis.yml](./.travis.yml) file.

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
