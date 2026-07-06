use duckdb::{Connection, Result};

#[test]
fn test_read_orc_local() -> Result<()> {
    let db = Connection::open_in_memory()?;

    // Enable unsigned extensions
    db.execute_batch("SET allow_unsigned_extensions=true;")?;

    // Load extension via SQL
    db.execute_batch("LOAD 'build/debug/orc.duckdb_extension';")?;

    let count: i64 = db.query_row(
        "SELECT count(*) FROM read_orc('test/sql/test.orc')",
        [],
        |row| row.get(0),
    )?;
    assert!(count > 0, "Expected non-zero rows but got {count}");
    println!("✅ Basic ORC: {count} rows");

    // Test big file
    let (count2, avg, min, max): (i64, f64, f64, f64) = db.query_row(
        "SELECT count(*), avg(score), min(score), max(score) FROM read_orc('/tmp/big_test.orc')",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;

    assert_eq!(count2, 10000);
    println!(
        "✅ Big ORC: count={}, avg={:.2}, min={}, max={}",
        count2, avg, min, max
    );
    Ok(())
}
