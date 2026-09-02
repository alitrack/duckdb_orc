"""Test FROM 'file.orc' replacement scan syntax."""
import duckdb
import os

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{os.path.abspath('build/debug/extension/orc/orc.duckdb_extension')}';")

ORC = os.path.abspath("test/sql/test.orc")

# 1. FROM 'abs/path.orc'
n = con.execute(f"SELECT count(*) FROM '{ORC}'").fetchone()[0]
assert n == 10000, n
print("✅ FROM absolute path:", n, "rows")

# 2. FROM relative path (duckdb resolves relative to CWD = repo root)
n = con.execute("SELECT count(*) FROM './test/sql/test.orc'").fetchone()[0]
assert n == 10000, n
print("✅ FROM relative './test/sql/test.orc':", n, "rows")

# 3. Data correctness
row = con.execute("SELECT id, name FROM './test/sql/test.orc' LIMIT 1").fetchone()
assert row == (0, "user_0"), row
print("✅ data via FROM syntax:", row)

# 4. Non-ORC passthrough
con.execute("CREATE TABLE t AS SELECT 1 AS a")
assert con.execute("SELECT * FROM t").fetchone()[0] == 1
print("✅ regular table unaffected")

# 5. Glob pattern resolves via replacement scan (single-file glob = same count)
n = con.execute("SELECT count(*) FROM './test/sql/*.orc'").fetchone()[0]
assert n == 10000, n
print("✅ glob './test/sql/*.orc':", n, "rows")

print("\n🎉 ALL REPLACEMENT SCAN TESTS PASSED")
