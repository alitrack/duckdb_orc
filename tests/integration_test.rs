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

#[test]
fn test_write_orc_roundtrip() -> Result<()> {
    let db = open_test_db()?;
    db.execute_batch("LOAD 'build/debug/extension/orc/orc.duckdb_extension';")?;

    // Create source data with supported types (orc-rust writer: bool, ints,
    // floats, date, timestamp, varchar, blob). LIST/STRUCT/DECIMAL not yet
    // supported by the upstream writer.
    db.execute_batch(
        "CREATE TABLE src AS SELECT
            i AS id,
            'name_' || i AS name,
            i * 1.5::DOUBLE AS score,
            i % 2 = 0 AS even,
            DATE '2024-01-01' + INTERVAL (i) DAY AS dt,
            TIMESTAMP '2024-01-01 10:00:00' + INTERVAL (i) MINUTE AS ts
         FROM range(100) t(i);",
    )?;

    let out_path = "test/sql/roundtrip.orc";
    db.execute_batch(&format!("COPY src TO '{out_path}' (FORMAT orc);"))?;
    println!("✅ Wrote {out_path}");

    // Read it back. Column names come from the ORC file; the C API's COPY
    // bind does not expose source column names, so they are column_N.
    let (n, first_name): (i64, String) = db.query_row(
        &format!("SELECT count(*), min(column_1) FROM read_orc('{out_path}')"),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(n, 100);
    assert_eq!(first_name, "name_0");
    println!("✅ Roundtrip: count={n}, min(name)={first_name}");

    // Verify values match exactly
    let (src_sum, orc_sum): (f64, f64) = db.query_row(
        &format!(
            "SELECT (SELECT sum(score) FROM src), (SELECT sum(column_2) FROM read_orc('{out_path}'))"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(src_sum, orc_sum, "score sums should match");
    println!("✅ Score sum matches: {orc_sum}");

    // Verify date/timestamp round-trip
    let (src_max_dt, orc_max_dt): (String, String) = db.query_row(
        &format!(
            "SELECT (SELECT max(dt)::VARCHAR FROM src), (SELECT max(column_4)::VARCHAR FROM read_orc('{out_path}'))"
        ),
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    assert_eq!(src_max_dt, orc_max_dt, "date should round-trip");
    println!("✅ Date round-trip: {orc_max_dt}");

    // Cleanup
    std::fs::remove_file(out_path).ok();
    Ok(())
}

#[test]
fn test_write_orc_rejects_unsupported_types() -> Result<()> {
    let db = open_test_db()?;
    db.execute_batch("LOAD 'build/debug/extension/orc/orc.duckdb_extension';")?;

    // LIST type is not supported by the upstream orc-rust writer; the COPY
    // should fail with a clean binder error instead of crashing the process.
    db.execute_batch("CREATE TABLE bad AS SELECT [1,2,3] AS nums FROM range(3);")?;
    let err = db
        .execute_batch("COPY bad TO 'test/sql/bad.orc' (FORMAT orc);")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not supported"),
        "expected type-not-supported error, got: {msg}"
    );
    println!("✅ Rejected unsupported type cleanly: {msg}");
    Ok(())
}
