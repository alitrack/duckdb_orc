use arrow::array::RecordBatchReader;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vtab::{
        arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type},
        BindInfo, InitInfo, TableFunctionInfo, VTab,
    },
};
#[cfg(feature = "extension")]
use duckdb::{Connection, duckdb_entrypoint_c_api};
use orc_rust::ArrowReaderBuilder;
use std::{
    error::Error,
    fs::File,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use orc_rust::ArrowReader;

/// DuckDB's STANDARD_VECTOR_SIZE — batches must not exceed this.
#[cfg_attr(not(feature = "extension"), allow(dead_code))]
const BATCH_SIZE: usize = 2048;

/// Per-scan state: tracks current file index, completion flag, and active reader.
#[repr(C)]
struct OrcInitData {
    /// True when all files have been fully read.
    done: AtomicBool,
    /// Index of the file currently being read.
    file_index: AtomicUsize,
    /// Active reader for the current file path.
    reader: Mutex<Option<ArrowReader<File>>>,
}

/// Bound parameters: file paths (expanded from glob) and schema.
#[repr(C)]
struct OrcBindData {
    file_paths: Vec<String>,
    schema: Arc<arrow::datatypes::Schema>,
}

#[cfg_attr(not(feature = "extension"), allow(dead_code))]
struct OrcVTab;

impl VTab for OrcVTab {
    type BindData = OrcBindData;
    type InitData = OrcInitData;

    fn bind(bind: &BindInfo) -> std::result::Result<Self::BindData, Box<dyn Error>> {
        let param_count = bind.get_parameter_count();
        if param_count != 1 {
            return Err(format!(
                "read_orc: expected 1 parameter (file path or glob), got {param_count}"
            )
            .into());
        }
        let pattern = bind.get_parameter(0).to_string();

        let mut file_paths: Vec<String> = glob::glob(&pattern)
            .map_err(|e| format!("read_orc: invalid glob pattern '{pattern}': {e}"))?
            .filter_map(|entry| entry.ok())
            .filter_map(|path| path.to_str().map(String::from))
            .collect();

        if file_paths.is_empty() {
            return Err(format!("read_orc: no files found matching '{pattern}'").into());
        }

        file_paths.sort();

        // Peek at first file for schema
        let file = File::open(&file_paths[0])
            .map_err(|e| format!("read_orc: cannot open '{}': {e}", file_paths[0]))?;

        let reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| format!("read_orc: not a valid ORC file '{}': {e}", file_paths[0]))?
            .with_batch_size(BATCH_SIZE)
            .build();

        let schema = reader.schema();

        // Validate all matched files have the same schema
        for path in &file_paths[1..] {
            let f = File::open(path)
                .map_err(|e| format!("read_orc: cannot open '{path}': {e}"))?;
            let r = ArrowReaderBuilder::try_new(f)
                .map_err(|e| format!("read_orc: not a valid ORC file '{path}': {e}"))?
                .with_batch_size(BATCH_SIZE)
                .build();
            if r.schema() != schema {
                return Err(format!(
                    "read_orc: schema mismatch — '{}' has different columns than '{}'. All files in a glob must have identical schemas.",
                    path, file_paths[0]
                ).into());
            }
        }

        for field in schema.fields() {
            let duckdb_type = to_duckdb_logical_type(field.data_type())?;
            bind.add_result_column(field.name(), duckdb_type);
        }

        Ok(OrcBindData { file_paths, schema })
    }

    fn init(_: &InitInfo) -> std::result::Result<Self::InitData, Box<dyn Error>> {
        Ok(OrcInitData {
            done: AtomicBool::new(false),
            file_index: AtomicUsize::new(0),
            reader: Mutex::new(None),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let bind_data = func.get_bind_data();

        if init_data.done.load(Ordering::Relaxed) {
            output.set_len(0);
            return Ok(());
        }

        // Keep trying files until we get a batch or run out.
        loop {
            let mut reader_guard = init_data.reader.lock().unwrap();

            // Open reader for current file index if needed.
            if reader_guard.is_none() {
                let idx = init_data.file_index.load(Ordering::Relaxed);
                if idx >= bind_data.file_paths.len() {
                    init_data.done.store(true, Ordering::Relaxed);
                    output.set_len(0);
                    return Ok(());
                }

                let file_path = &bind_data.file_paths[idx];
                let file = File::open(file_path)
                    .map_err(|e| format!("read_orc: cannot open '{file_path}': {e}"))?;
                let reader = ArrowReaderBuilder::try_new(file)
                    .map_err(|e| format!("read_orc: failed to create reader for '{file_path}': {e}"))?
                    .with_batch_size(BATCH_SIZE)
                    .build();
                *reader_guard = Some(reader);
            }

            let reader = reader_guard.as_mut().unwrap();

            match reader.next() {
                Some(Ok(batch)) => {
                    drop(reader_guard);
                    record_batch_to_duckdb_data_chunk(&batch, output)?;
                    return Ok(());
                }
                Some(Err(e)) => {
                    let idx = init_data.file_index.load(Ordering::Relaxed);
                    init_data.done.store(true, Ordering::Relaxed);
                    return Err(format!(
                        "read_orc: error reading '{}': {e}",
                        bind_data.file_paths[idx]
                    )
                    .into());
                }
                None => {
                    // File exhausted — advance to next file.
                    *reader_guard = None;
                    drop(reader_guard);
                    init_data.file_index.fetch_add(1, Ordering::Relaxed);
                    // Loop: next iteration opens the next file.
                }
            }
        }
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

#[cfg(feature = "extension")]
#[duckdb_entrypoint_c_api(ext_name = "orc")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<OrcVTab>("read_orc")?;
    Ok(())
}
