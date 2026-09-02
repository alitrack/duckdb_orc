use duckdb::{
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    ffi,
    vtab::{
        arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type},
        BindInfo, InitInfo, TableFunctionInfo, VTab,
    },
};
#[cfg(feature = "extension")]
use duckdb::{Connection, duckdb_entrypoint_c_api};
use orc_rust::{
    projection::ProjectionMask,
    ArrowReader, ArrowReaderBuilder,
};
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

mod write;

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
    /// Sequential reader for the file currently being read (single-stripe
    /// fallback). Lives here — NOT in a `static` — so a query that ends early
    /// (e.g. LIMIT) doesn't poison the reader position for the next query.
    seq_reader: Mutex<Option<(usize, ArrowReader<File>)>>,
    /// Projected file-column indices in DuckDB output order (projection
    /// pushdown). Empty when DuckDB needs no columns at all (e.g. `count(*)`)
    /// — in that case only row counts are streamed, nothing is decoded.
    cols: Vec<usize>,
    /// count(*) fast path: rows left to emit for the file at `file_index`.
    rows_left: AtomicUsize,
}

/// Bound parameters: file paths (expanded from glob), schema, and stripe ranges.
#[repr(C)]
struct OrcBindData {
    file_paths: Vec<String>,
    schema: Arc<arrow::datatypes::Schema>,
    /// Per-file stripe byte ranges for parallel reads.
    file_stripe_ranges: Vec<Vec<Range<usize>>>,
    /// Per-file total row counts (metadata only), for the count(*) fast path.
    file_row_counts: Vec<usize>,
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
        let mut file_row_counts: Vec<usize> = Vec::with_capacity(file_paths.len());

        for path in &file_paths {
            let f = File::open(path)
                .map_err(|e| format!("read_orc: cannot open '{path}': {e}"))?;

            let builder = ArrowReaderBuilder::try_new(f)
                .map_err(|e| format!("read_orc: not a valid ORC file '{path}': {e}"))?;

            let file_meta = builder.file_metadata();
            let current_schema = builder.schema();

            file_row_counts.push(
                file_meta
                    .stripe_metadatas()
                    .iter()
                    .map(|s| s.number_of_rows() as usize)
                    .sum::<usize>(),
            );

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

        Ok(OrcBindData { file_paths, schema, file_stripe_ranges, file_row_counts })
    }

    fn init(init: &InitInfo) -> std::result::Result<Self::InitData, Box<dyn Error>> {
        // Projection pushdown: DuckDB tells us which file columns this query
        // needs (in output order). Empty means no columns at all (count(*) etc.)
        let cols: Vec<usize> = init
            .get_column_indices()
            .into_iter()
            .map(|i| i as usize)
            .collect();
        Ok(OrcInitData {
            done: AtomicBool::new(false),
            file_index: AtomicUsize::new(0),
            rx: Mutex::new(None),
            _handles: Mutex::new(Vec::new()),
            seq_reader: Mutex::new(None),
            cols,
            rows_left: AtomicUsize::new(0),
        })
    }

    fn supports_pushdown() -> bool {
        true
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

        // No columns requested (count(*), exists, ...): stream row counts from
        // metadata — no decode, no I/O beyond the footer already read at bind.
        if init_data.cols.is_empty() {
            return func_count_only(init_data, bind_data, output);
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
            launch_parallel_scan(init_data, bind_data, path, stripe_ranges, BATCH_SIZE)?;
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
    bind_data: &OrcBindData,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn Error>> {
    let rx_guard = init_data.rx.lock().unwrap();
    let rx = rx_guard.as_ref().unwrap();
    match rx.recv() {
        Ok(batch) => {
            drop(rx_guard);
            let batch = reorder_batch(batch, &init_data.cols, &bind_data.schema)?;
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
    let mut guard = init_data.seq_reader.lock().unwrap();
    let current_idx = init_data.file_index.load(Ordering::Relaxed);

    let needs_new = match guard.as_ref() {
        Some((idx, _)) => *idx != current_idx,
        None => true,
    };

    if needs_new {
        let path = &bind_data.file_paths[current_idx];
        let file = File::open(path)
            .map_err(|e| format!("read_orc: cannot open '{path}': {e}"))?;
        let mut builder = ArrowReaderBuilder::try_new(file)
            .map_err(|e| format!("read_orc: failed to create reader for '{path}': {e}"))?;
        // Decode only the projected columns (projection pushdown). Match by
        // top-level column NAME — ProjectionMask indices are ORC type ids
        // (DFS, 1-based), not schema positions.
        if init_data.cols.len() < bind_data.schema.fields().len() {
            let names: Vec<String> = init_data
                .cols
                .iter()
                .map(|&c| bind_data.schema.field(c).name().to_string())
                .collect();
            let mask = ProjectionMask::named_roots(
                builder.file_metadata().root_data_type(),
                &names,
            );
            builder = builder.with_projection(mask);
        }
        let reader = builder.with_batch_size(BATCH_SIZE).build();
        *guard = Some((current_idx, reader));
    }

    let (_, reader) = guard.as_mut().unwrap();

    match reader.next() {
        Some(Ok(batch)) => {
            let batch = reorder_batch(batch, &init_data.cols, &bind_data.schema)?;
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
    bind_data: &OrcBindData,
    path: &str,
    stripe_ranges: &[Range<usize>],
    batch_size: usize,
) -> Result<(), Box<dyn Error>> {
    let num_workers = parallel_stripe_count().min(stripe_ranges.len());
    let stripes_per_worker = (stripe_ranges.len() + num_workers - 1) / num_workers;

    // Bounded channel: buffer a few batches per worker
    let (tx, rx) = mpsc::sync_channel::<arrow::array::RecordBatch>(num_workers * 4);

    let path = path.to_string();
    let total_cols = bind_data.schema.fields().len();
    // Projected top-level column NAMES for the ORC projection mask (matched by
    // name because ProjectionMask indices are ORC type ids, not positions).
    // Empty = decode everything.
    let proj_names: Vec<String> = if init_data.cols.len() < total_cols {
        init_data
            .cols
            .iter()
            .map(|&c| bind_data.schema.field(c).name().to_string())
            .collect()
    } else {
        Vec::new()
    };
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
        let proj_names = proj_names.clone();

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
                    // Decode only the projected columns (projection pushdown).
                    let builder = if proj_names.is_empty() {
                        builder
                    } else {
                        let mask = ProjectionMask::named_roots(
                            builder.file_metadata().root_data_type(),
                            &proj_names,
                        );
                        builder.with_projection(mask)
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

// ── Projection helpers ─────────────────────────────────────────────────────

/// Emit row counts only (from stripe metadata) for queries that request no
/// columns — `count(*)`, `exists`, ... No stripe data is read or decoded.
fn func_count_only(
    init_data: &OrcInitData,
    bind_data: &OrcBindData,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn Error>> {
    loop {
        let idx = init_data.file_index.load(Ordering::Relaxed);
        if idx >= bind_data.file_paths.len() {
            init_data.done.store(true, Ordering::Relaxed);
            output.set_len(0);
            return Ok(());
        }
        let mut left = init_data.rows_left.load(Ordering::Relaxed);
        if left == 0 {
            let total = bind_data.file_row_counts[idx];
            if total == 0 {
                init_data.file_index.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            init_data.rows_left.store(total, Ordering::Relaxed);
            left = total;
        }
        let n = left.min(BATCH_SIZE);
        let remaining = left - n;
        init_data.rows_left.store(remaining, Ordering::Relaxed);
        if remaining == 0 {
            // Current file exhausted — next call starts the next file.
            init_data.file_index.fetch_add(1, Ordering::Relaxed);
        }
        output.set_len(n);
        return Ok(());
    }
}

/// Align a decoded (projected) batch with DuckDB's expected column order.
///
/// orc-rust emits projected columns in its own (mask) order; DuckDB's
/// projection pushdown expects chunk position *i* to hold file column
/// `cols[i]`. Columns are matched by name against the full bind schema, so
/// the mapping is correct regardless of decode order. Returns the batch
/// unchanged when it is already in the right order.
fn reorder_batch(
    batch: arrow::array::RecordBatch,
    cols: &[usize],
    full_schema: &arrow::datatypes::Schema,
) -> Result<arrow::array::RecordBatch, Box<dyn Error>> {
    // Full scan, file order — nothing to do.
    let n = full_schema.fields().len();
    if cols.len() == n && cols.iter().enumerate().all(|(i, &c)| c == i) {
        return Ok(batch);
    }

    let batch_schema = batch.schema();
    let batch_fields = batch_schema.fields();
    let full_names: Vec<&str> = full_schema.fields().iter().map(|f| f.name().as_str()).collect();
    let mut arrays = Vec::with_capacity(cols.len());
    let mut fields = Vec::with_capacity(cols.len());
    let mut identity = true;
    for (emit_pos, &file_col) in cols.iter().enumerate() {
        let want = full_names[file_col];
        let j = batch_fields
            .iter()
            .position(|f| f.name() == want)
            .ok_or_else(|| format!("read_orc: projected column '{want}' missing from decoded batch"))?;
        if j != emit_pos {
            identity = false;
        }
        arrays.push(batch.column(j).clone());
        fields.push(batch_fields[j].clone());
    }
    if identity {
        return Ok(batch);
    }
    arrow::array::RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), arrays)
        .map_err(|e| format!("read_orc: failed to reorder projected batch: {e}").into())
}

// ── Replacement scan ───────────────────────────────────────────────────────
//
// Lets users write `FROM 'file.orc'` / `FROM '*.orc'` (or `SELECT * FROM
// 'path.orc'`) without naming the read_orc function explicitly. DuckDB calls
// registered replacement scans when a base table reference looks like a file
// path; we rewrite `.orc`-suffixed names into read_orc(name).

#[cfg(feature = "extension")]
fn register_orc_replacement_scan(db: ffi::duckdb_database) {
    unsafe extern "C" fn orc_replacement_scan(
        info: ffi::duckdb_replacement_scan_info,
        table_name: *const std::os::raw::c_char,
        _extra_data: *mut std::ffi::c_void,
    ) {
        if info.is_null() || table_name.is_null() {
            return;
        }
        let name = unsafe { std::ffi::CStr::from_ptr(table_name) };
        let name_str = name.to_string_lossy();

        // Only claim paths that look like ORC files. Case-insensitive on the
        // extension; glob patterns (`*.orc`, `data/*.orc`) end with .orc too,
        // and read_orc handles the glob. Anything else passes through to other
        // replacement scans / the normal catalog lookup.
        let lower = name_str.to_lowercase();
        if !lower.ends_with(".orc") {
            return;
        }

        // Replace with read_orc('<name>')
        unsafe {
            ffi::duckdb_replacement_scan_set_function_name(info, c"read_orc".as_ptr());
            let value = ffi::duckdb_create_varchar(table_name);
            if !value.is_null() {
                ffi::duckdb_replacement_scan_add_parameter(info, value);
                // The C API copies the value into the scan; we still own ours.
                let mut v = value;
                ffi::duckdb_destroy_value(&mut v);
            }
        }
    }

    unsafe {
        ffi::duckdb_add_replacement_scan(db, Some(orc_replacement_scan), std::ptr::null_mut(), None);
    }
}

// ── Entrypoint ─────────────────────────────────────────────────────────────
//
// The `duckdb_entrypoint_c_api` macro doesn't expose the raw `duckdb_connection`
// handle needed to register a COPY function, so the C entrypoint is written by
// hand following the same protocol the macro generates (api struct init →
// get_database → open Connection → call the extension init function).

#[cfg(feature = "extension")]
pub unsafe fn extension_init(con: &Connection) -> Result<(), Box<dyn Error>> {
    con.register_table_function::<OrcVTab>("read_orc")?;
    Ok(())
}

#[cfg(feature = "extension")]
#[no_mangle]
pub unsafe extern "C" fn orc_init_c_api(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> bool {
    unsafe {
        match orc_init_internal(info, access) {
            Ok(true) => true,
            Ok(false) => false,
            Err(e) => {
                if let Some(set_error_fn) = (*access).set_error {
                    if let Ok(c) = std::ffi::CString::new(e.to_string()) {
                        set_error_fn(info, c.as_ptr());
                    }
                }
                false
            }
        }
    }
}

#[cfg(feature = "extension")]
unsafe fn orc_init_internal(
    info: ffi::duckdb_extension_info,
    access: *const ffi::duckdb_extension_access,
) -> Result<bool, Box<dyn Error>> {
    unsafe {
        // Initialize the extension API struct (same as the proc macro does).
        let have_api_struct = ffi::duckdb_rs_extension_api_init(info, access, "v1.2.0")?;
        if !have_api_struct {
            return Ok(false);
        }

        let get_database = (*access)
            .get_database
            .ok_or("get_database function pointer is null in duckdb_extension_access")?;
        let db_ptr = get_database(info);
        if db_ptr.is_null() {
            return Ok(false);
        }
        let db: ffi::duckdb_database = *db_ptr;

        // Register the replacement scan so `FROM 'file.orc'` / `SELECT * FROM
        // '*.orc'` resolves to read_orc(...) — same mechanism DuckDB uses for
        // parquet/csv/json.
        register_orc_replacement_scan(db);

        // Register the read table function via the high-level crate.
        let connection = Connection::open_from_raw(db.cast())?;
        extension_init(&connection)?;

        // Register the ORC COPY function on a connection to the same database.
        // DuckDB stores copy functions in the database catalog, so any
        // connection to this database sees them.
        let mut con: ffi::duckdb_connection = std::ptr::null_mut();
        let rc = ffi::duckdb_connect(db, &mut con);
        if rc != ffi::duckdb_state_DuckDBSuccess {
            return Err("write_orc: duckdb_connect failed during entrypoint".into());
        }
        let reg = write::register_copy_function(con);
        ffi::duckdb_disconnect(&mut con);
        reg?;

        Ok(true)
    }
}
