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
| Write support | ⚠️ | `orc-rust` 0.8+ now has `ArrowWriter` with full write support, **but compression is not yet implemented** (uncompressed ORC only). Write support will be added to `duckdb_orc` once `orc-rust` supports compressed writes. |
| Parallel scan | ❌ | Currently single-threaded sequential scan. For single large files, DuckDB's native Parquet reader is ~5x faster due to built-in row-group parallelism. See [Parallelism](#parallelism) below. |
| Community extension | ❌ | [PR submitted](https://github.com/duckdb/community-extensions/pull/2239), pending review. |

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

## Parallelism

`read_orc` currently scans files sequentially in a single thread. This is a known limitation:

- **Single file**: ~5x slower than DuckDB's native Parquet reader on large files (100M rows: 42s vs 7.6s). DuckDB's Parquet reader parallelizes across row groups natively.
- **Multi-file glob**: each file is read sequentially in alphabetical order.

### Workaround for better performance

For single-file performance, convert ORC to Parquet first:

```bash
python3 -c "
import pyarrow.orc as orc; import pyarrow.parquet as pq
pq.write_table(orc.read_table('data.orc'), 'data.parquet')
"
```

Then query the Parquet file in DuckDB — it will benefit from native row-group parallelism.

### Future improvements

ORC files are organized in **stripes** (analogous to Parquet row groups). `orc-rust` exposes `with_file_byte_range()` for reading specific byte ranges, which could enable stripe-level parallelism in a future version of `duckdb_orc`.

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
