use arrow::array::RecordBatchReader;
use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{
        arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type},
        BindInfo, InitInfo, TableFunctionInfo, VTab,
    },
    Connection, Result,
};
use orc_rust::ArrowReaderBuilder;
use std::{
    error::Error,
    fs::File,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use orc_rust::ArrowReader;

/// DuckDB's STANDARD_VECTOR_SIZE — batches must not exceed this.
const BATCH_SIZE: usize = 2048;

/// Per-scan state: tracks whether the scan is done and holds the active reader.
#[repr(C)]
struct OrcInitData {
    done: AtomicBool,
    reader: Mutex<Option<ArrowReader<File>>>,
}

/// Bound parameters from the `read_orc(file_path)` call.
#[repr(C)]
struct OrcBindData {
    file_path: String,
    schema: Arc<arrow::datatypes::Schema>,
}

struct OrcVTab;

impl VTab for OrcVTab {
    type BindData = OrcBindData;
    type InitData = OrcInitData;

    fn bind(bind: &BindInfo) -> std::result::Result<Self::BindData, Box<dyn Error>> {
        let param_count = bind.get_parameter_count();
        if param_count != 1 {
            return Err(
                format!("read_orc: expected 1 parameter (file path), got {param_count}").into(),
            );
        }
        let file_path = bind.get_parameter(0).to_string();

        // Open file and peek at schema
        let file = File::open(&file_path)
            .map_err(|e| format!("read_orc: cannot open '{}': {e}", file_path))?;

        let reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| format!("read_orc: not a valid ORC file '{}': {e}", file_path))?
            .with_batch_size(BATCH_SIZE)
            .build();

        let schema = reader.schema();

        // Register columns with DuckDB
        for field in schema.fields() {
            let duckdb_type = to_duckdb_logical_type(field.data_type())?;
            bind.add_result_column(field.name(), duckdb_type);
        }

        Ok(OrcBindData {
            file_path,
            schema,
        })
    }

    fn init(_: &InitInfo) -> std::result::Result<Self::InitData, Box<dyn Error>> {
        Ok(OrcInitData {
            done: AtomicBool::new(false),
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

        let mut reader_guard = init_data.reader.lock().unwrap();

        // Lazy-init the reader on first call
        if reader_guard.is_none() {
            let file = File::open(&bind_data.file_path)
                .map_err(|e| format!("read_orc: cannot open '{}': {e}", bind_data.file_path))?;
            let reader = ArrowReaderBuilder::try_new(file)
                .map_err(|e| format!("read_orc: failed to create reader: {e}"))?
                .with_batch_size(BATCH_SIZE)
                .build();
            *reader_guard = Some(reader);
        }

        let reader = reader_guard.as_mut().unwrap();

        match reader.next() {
            Some(Ok(batch)) => {
                record_batch_to_duckdb_data_chunk(&batch, output)?;
            }
            Some(Err(e)) => {
                init_data.done.store(true, Ordering::Relaxed);
                return Err(
                    format!("read_orc: error reading '{}': {e}", bind_data.file_path).into(),
                );
            }
            None => {
                init_data.done.store(true, Ordering::Relaxed);
                output.set_len(0);
            }
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

/// Extension entry point — registers `read_orc` table function.
#[duckdb_entrypoint_c_api(ext_name = "orc")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<OrcVTab>("read_orc")?;
    Ok(())
}
