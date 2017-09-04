THRIFT_DIR=src/parquet_thrift

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
	# remove all generated files except mod.rs in THRIFT_DIR
	find ${THRIFT_DIR} -type f -name '*.rs' -not -name 'mod.rs' -print0 | xargs -0 rm --
