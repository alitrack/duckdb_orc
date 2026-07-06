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
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// Per-scan state: tracks whether we've produced output.
#[repr(C)]
struct OrcInitData {
    done: AtomicBool,
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

        let file = File::open(&bind_data.file_path).map_err(|e| {
            format!("read_orc: cannot re-open '{}': {e}", bind_data.file_path)
        })?;

        let mut reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| format!("read_orc: failed to create reader: {e}"))?
            .build();

        // Emit the first batch. For multi-batch streaming, this would need
        // stateful iteration across multiple func() calls.
        match reader.next() {
            Some(Ok(batch)) => {
                record_batch_to_duckdb_data_chunk(&batch, output)?;
            }
            Some(Err(e)) => {
                return Err(
                    format!("read_orc: error reading '{}': {e}", bind_data.file_path).into(),
                );
            }
            None => {
                output.set_len(0);
            }
        }

        init_data.done.store(true, Ordering::Relaxed);
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
