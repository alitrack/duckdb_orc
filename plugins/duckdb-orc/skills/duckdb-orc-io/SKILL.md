---
name: duckdb-orc-io
description: >
  Read and write Apache ORC files in DuckDB with the orc extension: read_orc(),
  direct FROM 'file.orc' replacement scans, multi-file globs, and COPY TO orc.
  Use for ORC data lakes, Hive-style partitioned dirs, or converting between
  ORC and Parquet/CSV. Pure Rust — no Java, no liborc.
version: 0.2.1
---

# orc: reading & writing Apache ORC

Extension: `orc` (pure Rust on orc-rust). `INSTALL orc FROM community; LOAD orc;`

## Read

```sql
SELECT * FROM read_orc('sales.orc');

-- Replacement scan: .orc paths resolve automatically in FROM
SELECT * FROM 'data/sales.orc';
SELECT * FROM 'data/*.orc';              -- glob: all files must share schema

-- Aggregate (filters apply post-scan — see limitations)
SELECT region, SUM(amount)
FROM read_orc('sales.orc')
WHERE year = 2024
GROUP BY region;
```

Supported: all ORC data types (primitives, struct, list, map), all encodings
and compression codecs (Zlib, Snappy, LZO, LZ4, ZSTD), projection pushdown
(column pruning via the optimizer), 2048-row batch streaming, stripe-level
parallel scan across cores for multi-stripe files.

## Write

```sql
COPY tbl TO 'file.orc' (FORMAT orc);
```

ZSTD compression by default; 2048-row batch streaming.

## Limitations (know before promising)

| Feature | Status | Workaround |
|---|---|---|
| Filter/predicate pushdown | ❌ blocked on upstream DuckDB C API | filters still apply post-scan; projection pushdown works |
| Write: nested/decimal types | ⚠️ rejected cleanly | bool, ints, floats, date, timestamp, varchar, blob write OK; list/struct/map/decimal do not yet |
| Write: column names | ⚠️ `column_0, column_1, ...` | rename after read with a SELECT alias |

## Pattern — ORC ↔ Parquet conversion

```sql
COPY (SELECT * FROM read_orc('lake/sales.orc')) TO 'sales.parquet' (FORMAT parquet);
COPY (SELECT * FROM 'sales.parquet') TO 'sales.orc' (FORMAT orc);
```
