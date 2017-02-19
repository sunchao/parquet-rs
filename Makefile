THRIFT_DIR=src/thrift

.PHONY: all, clean, test

%.rs:
	cd ${THRIFT_DIR}; thrift --gen rs $*.thrift

all: parquet.rs
	cargo build

test:
	cargo test

clippy:
	cargo clean & cargo clippy

clean:
	cd src/thrift; rm *.rs
