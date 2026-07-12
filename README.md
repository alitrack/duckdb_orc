# DuckDB ORC Extension (Rust)

A DuckDB extension for reading [Apache ORC](https://orc.apache.org) files, written in pure Rust.

Built on top of [`orc-rust`](https://github.com/datafusion-contrib/orc-rust), a native Rust ORC reader.

## Features

- `read_orc(file_path)` — read `.orc` files directly into DuckDB
- All ORC data types: primitives, struct, list, map
- All ORC encodings and compression codecs (Zlib, Snappy, LZO, LZ4, ZSTD)
- **Projection pushdown** — column pruning via DuckDB's optimizer
- **Multi-batch streaming** — files of any size, read in 2048-row batches

## Limitations

| Feature | Status | Notes |
|---|---|---|
| Filter / predicate pushdown | ❌ | **Blocked** — DuckDB C extension API does not expose `duckdb_table_function_supports_filter_pushdown`. Requires upstream DuckDB change. Workaround: DuckDB still applies filters after scan; projection pushdown works. |
| Multi-file / glob | ✅ | `read_orc('data/*.orc')` — all files must have identical schema. |
| Write support | ❌ | Read-only. |
| Community extension | ❌ | Not yet submitted to `duckdb/community-extensions`. |

## Installation

```sql
-- Load from local build (unsigned — requires `duckdb -unsigned`)
LOAD './build/debug/extension/orc/orc.duckdb_extension';

-- Read an ORC file
SELECT * FROM read_orc('data.orc');
```

## Usage

```sql
-- Basic read
SELECT * FROM read_orc('sales.orc');

-- Filter and aggregate (DuckDB applies filters post-scan)
SELECT region, SUM(amount)
FROM read_orc('sales.orc')
WHERE year = 2024
GROUP BY region;
```

## Building

Prerequisites: Rust toolchain, Python 3, Make.

```bash
git clone git@github.com:alitrack/duckdb_orc.git
cd duckdb_orc

# Debug build
make debug

# Release build (optimized, ~10MB)
make release

# Rust integration test (client mode, not loadable-extension mode)
cargo test --test integration_test -- --nocapture
```

## Development notes

This repository intentionally separates **extension build mode** from **Rust client/test mode**:

- `make debug` / `make release` build the loadable DuckDB extension with `--features extension`
- `cargo test` runs without that feature so `duckdb::Connection::open_in_memory()` works normally

Why this matters:

- `duckdb/loadable-extension` replaces parts of the regular DuckDB client API with extension-entrypoint wrappers
- if that feature is enabled during normal Rust tests, `Connection::open_in_memory()` can fail with:
  - `DuckDB API not initialized or DuckDB feature omitted`

Recommended workflow:

1. Build the extension with `make debug` or `make release`
2. Run Rust integration tests with plain `cargo test ...`
3. Optionally verify the built artifact in DuckDB CLI with `duckdb -unsigned`

Load with DuckDB (requires `-unsigned` for locally-built extensions):

```bash
duckdb -unsigned
```

```sql
LOAD './build/debug/extension/orc/orc.duckdb_extension';
SELECT * FROM read_orc('test.orc');
```

## Credits

- [`orc-rust`](https://github.com/datafusion-contrib/orc-rust) — native Rust ORC reader (Arrow output)
- [`duckdb-rs`](https://github.com/duckdb/duckdb-rs) — Rust bindings for DuckDB
- [`quack-rs`](https://github.com/tomtom215/quack-rs) — alternative Rust SDK for DuckDB extensions (evaluated, not adopted)

## License

MIT
