.PHONY: clean debug release test

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

debug: build/debug/extension/orc/orc.duckdb_extension

release: build/release/extension/orc/orc.duckdb_extension

build/debug/extension/orc/orc.duckdb_extension: target/debug/libduckdb_orc.so scripts/metadata.py
	@mkdir -p $(dir $@)
	@python3 $(PROJ_DIR)scripts/metadata.py $< -o $@
	@echo "  → $@"

build/release/extension/orc/orc.duckdb_extension: target/release/libduckdb_orc.so scripts/metadata.py
	@mkdir -p $(dir $@)
	@python3 $(PROJ_DIR)scripts/metadata.py $< -o $@
	@echo "  → $@"

target/debug/libduckdb_orc.so:
	cargo build

target/release/libduckdb_orc.so:
	cargo build --release

test: debug
	@duckdb -unsigned -c "LOAD 'build/debug/extension/orc/orc.duckdb_extension'; SELECT 'OK' as status, version() as duckdb_version;"

clean:
	cargo clean
	rm -rf build

.PHONY: debug release test clean
