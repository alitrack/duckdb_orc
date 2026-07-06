# DuckDB ORC Extension (Rust)

A DuckDB extension for reading [Apache ORC](https://orc.apache.org) files, written in pure Rust.

Built on top of [`orc-rust`](https://github.com/datafusion-contrib/orc-rust), a native Rust ORC reader.

## Features

- `read_orc(file_path)` — read `.orc` files directly into DuckDB
- All ORC data types (primitive, struct, list, map)
- All compression codecs (Zlib, Snappy, LZO, LZ4, ZSTD)
- Projection pushdown via DuckDB's column pruning

## Installation

```sql
-- Load from local build
LOAD './build/debug/extension/orc/orc.duckdb_extension';

-- Read an ORC file
SELECT * FROM read_orc('data.orc');
```

## Usage

```sql
-- Basic read
SELECT * FROM read_orc('sales.orc');

-- Filter and aggregate
SELECT region, SUM(amount) 
FROM read_orc('sales.orc') 
WHERE year = 2024 
GROUP BY region;

-- Multiple files (via glob)
SELECT * FROM read_orc('data/2024/*.orc');
```

## Building

Prerequisites: Rust toolchain, Python 3, Make, Git.

```bash
git clone --recurse-submodules https://github.com/alitrack/duckdb_orc.git
cd duckdb_orc
make configure
make debug
```

Then load with DuckDB:

```bash
duckdb -unsigned
```

```sql
LOAD './build/debug/extension/orc/orc.duckdb_extension';
```

## Credits

- [`orc-rust`](https://github.com/datafusion-contrib/orc-rust) — native Rust ORC reader
- [`duckdb-rs`](https://github.com/duckdb/duckdb-rs) — Rust bindings for DuckDB
- [`duckdb/extension-template-rs`](https://github.com/duckdb/extension-template-rs) — Rust extension template

## License

MIT
