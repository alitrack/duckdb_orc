# DuckDB ORC Extension (Rust)

A DuckDB extension for reading and writing [Apache ORC](https://orc.apache.org) files, written in pure Rust.

Built on top of [`orc-rust`](https://github.com/datafusion-contrib/orc-rust), a native Rust ORC reader/writer.

## Features

### Read
- `read_orc(file_path)` — read `.orc` files directly into DuckDB
- All ORC data types: primitives, struct, list, map
- All ORC encodings and compression codecs (Zlib, Snappy, LZO, LZ4, ZSTD)
- **Projection pushdown** — column pruning via DuckDB's optimizer
- **Multi-batch streaming** — files of any size, read in 2048-row batches
- **Stripe-level parallel scan** — large files read across CPU cores (multi-stripe files only)

### Write
- `COPY tbl TO 'file.orc' (FORMAT orc)` — write any query result to ORC
- **ZSTD compression** by default (switchable in source)
- 2048-row batch streaming

## Limitations

| Feature | Status | Notes |
|---|---|---|
| Filter / predicate pushdown | ❌ | **Blocked** — DuckDB C extension API does not expose `duckdb_table_function_supports_filter_pushdown`. Requires upstream DuckDB change. Workaround: DuckDB still applies filters after scan; projection pushdown works. |
| Multi-file / glob | ✅ | `read_orc('data/*.orc')` — all files must have identical schema. |
| Write — types | ⚠️ | Writer supports bool, int8/16/32/64, float32/64, date, timestamp, varchar, blob. **List/Struct/Map/Decimal rejected cleanly** — upstream `orc-rust` writer doesn't encode them yet. |
| Write — column names | ⚠️ | Output columns are `column_0, column_1, ...`. The DuckDB C API's COPY bind does not expose source column names, so they cannot be preserved yet. |
| Parallel scan | ✅ | Multi-stripe files read in parallel across threads (caps at CPU count). |

## Installation

```sql
-- Install from the DuckDB community extension registry (v0.2.0+, all platforms)
INSTALL orc FROM community;
LOAD orc;

-- Read an ORC file
SELECT * FROM read_orc('data.orc');
```

For local development builds (unsigned — requires `duckdb -unsigned`):

```sql
LOAD './build/debug/extension/orc/orc.duckdb_extension';
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

-- Write a table to ORC
COPY sales TO 'sales_copy.orc' (FORMAT orc);

-- Write any query result
COPY (SELECT region, SUM(amount) AS total FROM sales GROUP BY region)
TO 'region_totals.orc' (FORMAT orc);
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
```

**Note on `orc-rust`:** the writer needs `orc-rust` at commit `2060e243` or newer
(compressed writes landed upstream on 2026-08-18). The `Cargo.toml` in this repo
pins the required revision.

## Development notes

This repository intentionally separates **extension build mode** from **Rust client/test mode**:

- `make debug` / `make release` build the DuckDB extension (feature `extension`)
- `cargo test` builds the client-side test harness (no extension entrypoint)

The COPY write path uses the DuckDB C API's `duckdb_copy_function` interface
(bind → global_init → sink → finalize). It requires `duckdb` crate ≥ 1.10505
(the C API copy-function surface landed there).
