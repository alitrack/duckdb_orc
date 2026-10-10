# duckdb-orc — Claude Code Plugin

One skill giving your AI assistant verified knowledge of the
[duckdb_orc](https://github.com/alitrack/duckdb_orc) extension: ORC reads
(read_orc, replacement scans, globs), COPY TO orc writes, and the current
limitation table.

## Install

```text
/plugin marketplace add alitrack/duckdb_orc
/plugin install duckdb-orc@duckdb-orc
```

## Skills

| Skill | Covers |
|---|---|
| `duckdb-orc-io` | read/write API, glob scans, limitations, ORC↔Parquet conversion |

```sql
INSTALL orc FROM community; LOAD orc;
```

MIT, alitrack.
