.PHONY: clean clean_all debug release

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
EXTENSION_NAME := orc
SO_FILE := $(PROJ_DIR)target/debug/libduckdb_orc.so
OUTPUT := $(PROJ_DIR)build/debug/extension/orc/orc.duckdb_extension

all: debug

debug:
	cargo build
	mkdir -p $(dir $(OUTPUT))
	python3 $(PROJ_DIR)scripts/metadata.py $(SO_FILE) -o $(OUTPUT)

release:
	cargo build --release
	mkdir -p $(PROJ_DIR)build/release/extension/orc/
	python3 $(PROJ_DIR)scripts/metadata.py $(PROJ_DIR)target/release/libduckdb_orc.so -o $(PROJ_DIR)build/release/extension/orc/orc.duckdb_extension

test:
	@echo "Tests require DuckDB with -unsigned flag"
	@echo "Run: duckdb -unsigned -c \"LOAD '$(OUTPUT)'; SELECT * FROM read_orc('test.orc');\""

clean:
	cargo clean
	rm -rf build

.PHONY: debug release test clean all
