use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    vtab::{
        arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type},
        BindInfo, InitInfo, TableFunctionInfo, VTab,
    },
};
#[cfg(feature = "extension")]
use duckdb::{Connection, duckdb_entrypoint_c_api};
use orc_rust::{ArrowReader, ArrowReaderBuilder};
use std::{
    error::Error,
    fs::File,
    ops::Range,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver},
    },
    thread::{self, JoinHandle},
};

/// DuckDB's STANDARD_VECTOR_SIZE — batches must not exceed this.
#[cfg_attr(not(feature = "extension"), allow(dead_code))]
const BATCH_SIZE: usize = 2048;

/// Number of stripes to read in parallel. Caps at CPU count.
fn parallel_stripe_count() -> usize {
    thread::available_parallelism()
        .map(|n| n.get().max(1))
        .unwrap_or(4)
}

/// Per-scan state: tracks completion and manages the parallel read pipeline.
#[repr(C)]
struct OrcInitData {
    /// True when all files have been fully read.
    done: AtomicBool,
    /// Index of the file currently being read.
    file_index: AtomicUsize,
    /// Receives RecordBatches from worker threads. None before scan starts.
    rx: Mutex<Option<Receiver<arrow::array::RecordBatch>>>,
    /// Worker thread handles, joined on completion.
    _handles: Mutex<Vec<JoinHandle<()>>>,
}

/// Bound parameters: file paths (expanded from glob), schema, and stripe ranges.
#[repr(C)]
struct OrcBindData {
    file_paths: Vec<String>,
    schema: Arc<arrow::datatypes::Schema>,
    /// Per-file stripe byte ranges for parallel reads.
    file_stripe_ranges: Vec<Vec<Range<usize>>>,
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

        let mut schema: Option<Arc<arrow::datatypes::Schema>> = None;
        let mut file_stripe_ranges: Vec<Vec<Range<usize>>> = Vec::with_capacity(file_paths.len());

        for path in &file_paths {
            let f = File::open(path)
                .map_err(|e| format!("read_orc: cannot open '{path}': {e}"))?;

            let builder = ArrowReaderBuilder::try_new(f)
                .map_err(|e| format!("read_orc: not a valid ORC file '{path}': {e}"))?;

            let file_meta = builder.file_metadata();
            let current_schema = builder.schema();

            // Validate schema consistency across files
            match &schema {
                Some(existing) => {
                    if current_schema != *existing {
                        return Err(format!(
                            "read_orc: schema mismatch — '{}' has different columns than '{}'",
                            path, file_paths[0]
                        )
                        .into());
                    }
                }
                None => {
                    schema = Some(current_schema);
                }
            }

            // Extract stripe byte ranges for parallel reading
            let stripes: Vec<Range<usize>> = file_meta
                .stripe_metadatas()
                .iter()
                .map(|s| {
                    let start = s.offset() as usize;
                    let end = (s.offset() + s.index_length() + s.data_length()
                        + s.footer_length()) as usize;
                    start..end
                })
                .collect();

            file_stripe_ranges.push(stripes);
        }

        let schema = schema.unwrap();

        for field in schema.fields() {
            let duckdb_type = to_duckdb_logical_type(field.data_type())?;
            bind.add_result_column(field.name(), duckdb_type);
        }

        Ok(OrcBindData { file_paths, schema, file_stripe_ranges })
    }

    fn init(_: &InitInfo) -> std::result::Result<Self::InitData, Box<dyn Error>> {
        Ok(OrcInitData {
            done: AtomicBool::new(false),
            file_index: AtomicUsize::new(0),
            rx: Mutex::new(None),
            _handles: Mutex::new(Vec::new()),
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

        // Try to pull a batch from the current parallel scan channel.
        {
            let rx_guard = init_data.rx.lock().unwrap();
            if rx_guard.is_some() {
                drop(rx_guard);
                return poll_channel(init_data, bind_data, output);
            }
        }

        // No active scan — start one for the current file.
        loop {
            let idx = init_data.file_index.load(Ordering::Relaxed);
            if idx >= bind_data.file_paths.len() {
                init_data.done.store(true, Ordering::Relaxed);
                output.set_len(0);
                return Ok(());
            }

            let path = &bind_data.file_paths[idx];
            let stripe_ranges = &bind_data.file_stripe_ranges[idx];

            if stripe_ranges.is_empty() || stripe_ranges.len() == 1 {
                // Single stripe — read sequentially (no parallelism benefit).
                return read_sequential(init_data, bind_data, output);
            }

            // Multiple stripes — launch parallel scan.
            launch_parallel_scan(init_data, path, stripe_ranges, BATCH_SIZE)?;
            return poll_channel(init_data, bind_data, output);
        }
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }
}

// ── Channel-based parallel scan ────────────────────────────────────────────

/// Poll the active channel for a RecordBatch. If exhausted, advance to next file.
fn poll_channel(
    init_data: &OrcInitData,
    _bind_data: &OrcBindData,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn Error>> {
    let rx_guard = init_data.rx.lock().unwrap();
    let rx = rx_guard.as_ref().unwrap();
    match rx.recv() {
        Ok(batch) => {
            drop(rx_guard);
            record_batch_to_duckdb_data_chunk(&batch, output)?;
            Ok(())
        }
        Err(_) => {
            // Channel closed — all stripes for this file are done.
            drop(rx_guard);
            // Join worker threads
            let handles = std::mem::take(&mut *init_data._handles.lock().unwrap());
            for h in handles {
                let _ = h.join();
            }
            *init_data.rx.lock().unwrap() = None;
            init_data.file_index.fetch_add(1, Ordering::Relaxed);
            output.set_len(0);
            Ok(())
        }
    }
}

// ── Sequential fallback ────────────────────────────────────────────────────

/// Read a single-stripe (or empty) file sequentially.
fn read_sequential(
    init_data: &OrcInitData,
    bind_data: &OrcBindData,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn Error>> {
    // Static guard so we don't open/close the same file repeatedly.
    static SEQ: Mutex<Option<(usize, ArrowReader<File>)>> = Mutex::new(None);

    let mut guard = SEQ.lock().unwrap();
    let current_idx = init_data.file_index.load(Ordering::Relaxed);

    let needs_new = match guard.as_ref() {
        Some((idx, _)) => *idx != current_idx,
        None => true,
    };

    if needs_new {
        let path = &bind_data.file_paths[current_idx];
        let file = File::open(path)
            .map_err(|e| format!("read_orc: cannot open '{path}': {e}"))?;
        let reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| format!("read_orc: failed to create reader for '{path}': {e}"))?
            .with_batch_size(BATCH_SIZE)
            .build();
        *guard = Some((current_idx, reader));
    }

    let (_, reader) = guard.as_mut().unwrap();

    match reader.next() {
        Some(Ok(batch)) => {
            record_batch_to_duckdb_data_chunk(&batch, output)?;
            Ok(())
        }
        Some(Err(e)) => {
            init_data.done.store(true, Ordering::Relaxed);
            Err(format!(
                "read_orc: error reading '{}': {e}",
                bind_data.file_paths[current_idx]
            )
            .into())
        }
        None => {
            *guard = None;
            drop(guard);
            init_data.file_index.fetch_add(1, Ordering::Relaxed);
            output.set_len(0);
            Ok(())
        }
    }
}

// ── Parallel stripe scan ───────────────────────────────────────────────────

/// Launch worker threads to read individual stripes in parallel.
fn launch_parallel_scan(
    init_data: &OrcInitData,
    path: &str,
    stripe_ranges: &[Range<usize>],
    batch_size: usize,
) -> Result<(), Box<dyn Error>> {
    let num_workers = parallel_stripe_count().min(stripe_ranges.len());
    let stripes_per_worker = (stripe_ranges.len() + num_workers - 1) / num_workers;

    // Bounded channel: buffer a few batches per worker
    let (tx, rx) = mpsc::sync_channel::<arrow::array::RecordBatch>(num_workers * 4);

    let path = path.to_string();
    let mut handles = Vec::with_capacity(num_workers);

    for worker_id in 0..num_workers {
        let start = worker_id * stripes_per_worker;
        let end = (start + stripes_per_worker).min(stripe_ranges.len());
        if start >= end {
            break;
        }

        let tx = tx.clone();
        let path = path.clone();
        let ranges: Vec<Range<usize>> = stripe_ranges[start..end].to_vec();

        let handle = thread::Builder::new()
            .name(format!("orc-w{}", worker_id))
            .spawn(move || {
                for range in &ranges {
                    let file = match File::open(&path) {
                        Ok(f) => f,
                        Err(_) => return,
                    };
                    let builder = match ArrowReaderBuilder::try_new(file) {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    let mut reader = builder
                        .with_file_byte_range(range.clone())
                        .with_batch_size(batch_size)
                        .build();

                    while let Some(result) = reader.next() {
                        match result {
                            Ok(batch) => {
                                if tx.send(batch).is_err() {
                                    return; // consumer dropped
                                }
                            }
                            Err(_) => return,
                        }
                    }
                }
            })
            .map_err(|e| format!("read_orc: failed to spawn worker: {e}"))?;

        handles.push(handle);
    }

    *init_data._handles.lock().unwrap() = handles;
    *init_data.rx.lock().unwrap() = Some(rx);

    Ok(())
}

#[cfg(feature = "extension")]
#[duckdb_entrypoint_c_api(ext_name = "orc")]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<OrcVTab>("read_orc")?;
    Ok(())
}
