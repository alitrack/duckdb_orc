use duckdb::{Config, Connection, Result};

fn open_test_db() -> Result<Connection> {
    let config = Config::default().allow_unsigned_extensions()?;
    Connection::open_in_memory_with_flags(config)
}

#[test]
fn test_read_orc_local() -> Result<()> {
    let db = open_test_db()?;

    // Load extension via SQL
    db.execute_batch("LOAD 'build/debug/extension/orc/orc.duckdb_extension';")?;

    let count: i64 = db.query_row(
        "SELECT count(*) FROM read_orc('test/sql/test.orc')",
        [],
        |row| row.get(0),
    )?;
    assert!(count > 0, "Expected non-zero rows but got {count}");
    println!("✅ Basic ORC: {count} rows");

    let (count2, avg, min, max): (i64, f64, f64, f64) = db.query_row(
        "SELECT count(*), avg(score), min(score), max(score) FROM read_orc('test/sql/test.orc')",
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
    )?;

    assert_eq!(count2, 10000);
    assert_eq!(avg, 7499.25);
    assert_eq!(min, 0.0);
    assert_eq!(max, 14998.5);
    println!(
        "✅ Big ORC: count={}, avg={:.2}, min={}, max={}",
        count2, avg, min, max
    );
    Ok(())
}
