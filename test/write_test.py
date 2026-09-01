"""Direct integration test for duckdb_orc write support (uses Python duckdb as driver)."""
import duckdb
import os
import sys

OUT = "/tmp/orc_roundtrip.orc"

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{os.path.abspath('build/debug/extension/orc/orc.duckdb_extension')}';")

# Source table with all supported types
con.execute("""
CREATE TABLE src AS SELECT
    i AS id,
    'name_' || i AS name,
    i * 1.5::DOUBLE AS score,
    i % 2 = 0 AS even,
    DATE '2024-01-01' + INTERVAL (i) DAY AS dt,
    TIMESTAMP '2024-01-01 10:00:00' + INTERVAL (i) MINUTE AS ts
FROM range(100) t(i);
""")

# Write
con.execute(f"COPY src TO '{OUT}' (FORMAT orc);")
print("✅ wrote", OUT, os.path.getsize(OUT), "bytes")

# Read back
n = con.execute(f"SELECT count(*) FROM read_orc('{OUT}')").fetchone()[0]
assert n == 100, f"expected 100 rows, got {n}"
print("✅ rows:", n)

first = con.execute(f"SELECT min(column_1) FROM read_orc('{OUT}')").fetchone()[0]
assert first == "name_0", first
print("✅ min(name):", first)

src_sum = con.execute("SELECT sum(score) FROM src").fetchone()[0]
orc_sum = con.execute(f"SELECT sum(column_2) FROM read_orc('{OUT}')").fetchone()[0]
assert src_sum == orc_sum, f"sum mismatch: {src_sum} vs {orc_sum}"
print("✅ sum(score):", orc_sum)

src_max_dt = con.execute("SELECT max(dt)::VARCHAR FROM src").fetchone()[0]
orc_max_dt = con.execute(f"SELECT max(column_4)::VARCHAR FROM read_orc('{OUT}')").fetchone()[0]
assert src_max_dt == orc_max_dt, f"date mismatch: {src_max_dt} vs {orc_max_dt}"
print("✅ max(dt):", orc_max_dt)

src_max_ts = con.execute("SELECT max(ts)::VARCHAR FROM src").fetchone()[0]
orc_max_ts = con.execute(f"SELECT max(column_5)::VARCHAR FROM read_orc('{OUT}')").fetchone()[0]
assert src_max_ts == orc_max_ts, f"ts mismatch: {src_max_ts} vs {orc_max_ts}"
print("✅ max(ts):", orc_max_ts)

# Unsupported type (LIST) must fail cleanly
con.execute("CREATE TABLE bad AS SELECT [1,2,3] AS nums FROM range(3);")
try:
    con.execute("COPY bad TO '/tmp/orc_bad.orc' (FORMAT orc);")
    print("❌ unsupported type did NOT error!")
    sys.exit(1)
except Exception as e:
    msg = str(e)
    assert "not supported" in msg, f"unexpected error: {msg}"
    print("✅ unsupported LIST rejected cleanly:", msg[:90])

os.remove(OUT)
print("\n🎉 ALL WRITE TESTS PASSED")
