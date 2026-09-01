//! COPY function implementation for writing ORC files.
//!
//! Registers a DuckDB COPY function named `orc` so that:
//! ```sql
//! COPY tbl TO 'file.orc' (FORMAT orc);
//! ```
//! writes the query result as an ORC file using orc-rust's ArrowWriter.

use std::{
    error::Error,
    ffi::{CStr, CString},
    fs::File,
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryBuilder, BooleanArray, BooleanBuilder, Decimal128Builder,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        ListArray, StringBuilder, StructArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    },
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
};
use duckdb::{
    core::LogicalTypeId,
    ffi::{
        self, duckdb_copy_function, duckdb_copy_function_bind_info,
        duckdb_copy_function_finalize_info, duckdb_copy_function_global_init_info,
        duckdb_copy_function_sink_info, duckdb_data_chunk, duckdb_logical_type,
        duckdb_string_t, duckdb_vector, idx_t,
    },
};
use orc_rust::{ArrowWriter, ArrowWriterBuilder, compression::CompressionType};

/// Per-COPY bind data: the Arrow schema of the output columns.
struct OrcCopyBindData {
    schema: SchemaRef,
}

/// Per-COPY global state: the open ORC writer.
/// Wrapped in `Option` so `finalize` can move the writer out for `close()`.
struct OrcCopyState {
    writer: Option<ArrowWriter<File>>,
}

/// Verify the schema only contains types the orc-rust writer can encode.
/// The upstream writer's `create_encoder` panics with `unimplemented!` on
/// unknown types — which would abort the whole DuckDB process — so we must
/// reject unsupported types at bind time with a clean SQL error.
fn validate_schema_for_write(schema: &Schema) -> Result<(), Box<dyn Error>> {
    fn check(data_type: &DataType) -> Result<(), Box<dyn Error>> {
        match data_type {
            DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary => Ok(()),
            DataType::Date32 => Ok(()),
            DataType::Timestamp(_, tz) => {
                // Upstream only accepts None or "UTC" timezones.
                if matches!(tz.as_deref(), None | Some("UTC")) {
                    Ok(())
                } else {
                    Err(format!(
                        "write_orc: unsupported timestamp timezone {tz:?} (only UTC or naive supported)"
                    )
                    .into())
                }
            }
            other => Err(format!(
                "write_orc: type {other:?} not supported by the ORC writer yet \
                 (supported: bool, int8-64, float32/64, date, timestamp, varchar, blob)"
            )
            .into()),
        }
    }
    for field in schema.fields() {
        check(field.data_type())?;
    }
    Ok(())
}

/// Convert a `duckdb_logical_type` to an Arrow `DataType`.
///
/// # Safety
/// `logical_type` must be a valid `duckdb_logical_type` owned by the caller.
pub unsafe fn logical_type_to_arrow(logical_type: duckdb_logical_type) -> Result<DataType, Box<dyn Error>> {
    let type_id = ffi::duckdb_get_type_id(logical_type);
    let id = LogicalTypeId::from(type_id);
    match id {
        LogicalTypeId::Boolean => Ok(DataType::Boolean),
        LogicalTypeId::Tinyint => Ok(DataType::Int8),
        LogicalTypeId::Smallint => Ok(DataType::Int16),
        LogicalTypeId::Integer => Ok(DataType::Int32),
        LogicalTypeId::Bigint => Ok(DataType::Int64),
        LogicalTypeId::UTinyint => Ok(DataType::UInt8),
        LogicalTypeId::USmallint => Ok(DataType::UInt16),
        LogicalTypeId::UInteger => Ok(DataType::UInt32),
        LogicalTypeId::UBigint => Ok(DataType::UInt64),
        LogicalTypeId::Float => Ok(DataType::Float32),
        LogicalTypeId::Double => Ok(DataType::Float64),
        LogicalTypeId::Timestamp => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        LogicalTypeId::TimestampS => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        LogicalTypeId::TimestampMs => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        LogicalTypeId::TimestampNs => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        LogicalTypeId::TimestampTZ => Ok(DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
        LogicalTypeId::Date => Ok(DataType::Date32),
        LogicalTypeId::Time => Ok(DataType::Time64(TimeUnit::Microsecond)),
        LogicalTypeId::Varchar => Ok(DataType::Utf8),
        LogicalTypeId::Blob => Ok(DataType::Binary),
        LogicalTypeId::Decimal => {
            // duckdb_get_decimal_info returns width/scale packed
            let width = 18;
            let scale = ffi::duckdb_decimal_scale(logical_type) as i8;
            let _ = width;
            Ok(DataType::Decimal128(38, scale))
        }
        LogicalTypeId::List => {
            let mut child_type = ffi::duckdb_list_type_child_type(logical_type);
            if child_type.is_null() {
                return Err("duckdb_list_type_child_type returned NULL".into());
            }
            let child = logical_type_to_arrow(child_type)?;
            ffi::duckdb_destroy_logical_type(&mut child_type);
            Ok(DataType::List(Arc::new(Field::new("item", child, true))))
        }
        LogicalTypeId::Struct => {
            let child_count = ffi::duckdb_struct_type_child_count(logical_type);
            let mut fields = Vec::with_capacity(child_count as usize);
            for i in 0..child_count {
                let name = ffi::duckdb_struct_type_child_name(logical_type, i);
                let name_str = if name.is_null() {
                    format!("field_{i}")
                } else {
                    let s = CStr::from_ptr(name).to_string_lossy().into_owned();
                    ffi::duckdb_free(name as *mut _);
                    s
                };
                let mut child_type = ffi::duckdb_struct_type_child_type(logical_type, i);
                if child_type.is_null() {
                    return Err(format!("duckdb_struct_type_child_type({i}) returned NULL").into());
                }
                let child = logical_type_to_arrow(child_type)?;
                ffi::duckdb_destroy_logical_type(&mut child_type);
                fields.push(Field::new(name_str, child, true));
            }
            Ok(DataType::Struct(fields.into()))
        }
        other => Err(format!("write_orc: unsupported DuckDB type {other:?}").into()),
    }
}

/// Convert a DuckDB data chunk to an Arrow RecordBatch.
///
/// # Safety
/// `chunk` must be a valid `duckdb_data_chunk` for the duration of the call.
pub unsafe fn chunk_to_record_batch(
    chunk: duckdb_data_chunk,
    schema: &Schema,
) -> Result<arrow::record_batch::RecordBatch, Box<dyn Error>> {
    let ncols = ffi::duckdb_data_chunk_get_column_count(chunk);
    let nrows = ffi::duckdb_data_chunk_get_size(chunk);
    if schema.fields().len() != ncols as usize {
        return Err(format!(
            "write_orc: chunk has {ncols} columns but schema has {}",
            schema.fields().len()
        )
        .into());
    }

    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols as usize);
    for i in 0..ncols {
        let vector = ffi::duckdb_data_chunk_get_vector(chunk, i);
        let array = vector_to_arrow_array(vector, nrows as usize, schema.field(i as usize).data_type())?;
        arrays.push(array);
    }
    arrow::record_batch::RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| format!("write_orc: failed to build RecordBatch: {e}").into())
}

/// # Safety
/// `vector` must be valid for `len` rows; `data_type` must match the vector's logical type.
unsafe fn vector_to_arrow_array(
    vector: duckdb_vector,
    len: usize,
    data_type: &DataType,
) -> Result<ArrayRef, Box<dyn Error>> {
    let data = ffi::duckdb_vector_get_data(vector);
    let validity = ffi::duckdb_vector_get_validity(vector);

    match data_type {
        DataType::Boolean => {
            let mut builder = arrow::array::BooleanBuilder::new();
            let slice = std::slice::from_raw_parts(data as *const u8, len);
            for i in 0..len {
                if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                    builder.append_value(slice[i] != 0);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => Ok(Arc::new(Int8Array::from(
            read_primitive::<i8>(data, validity, len)?,
        ))),
        DataType::Int16 => Ok(Arc::new(Int16Array::from(
            read_primitive::<i16>(data, validity, len)?,
        ))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(
            read_primitive::<i32>(data, validity, len)?,
        ))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(
            read_primitive::<i64>(data, validity, len)?,
        ))),
        DataType::UInt8 => Ok(Arc::new(UInt8Array::from(
            read_primitive::<u8>(data, validity, len)?,
        ))),
        DataType::UInt16 => Ok(Arc::new(UInt16Array::from(
            read_primitive::<u16>(data, validity, len)?,
        ))),
        DataType::UInt32 => Ok(Arc::new(UInt32Array::from(
            read_primitive::<u32>(data, validity, len)?,
        ))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(
            read_primitive::<u64>(data, validity, len)?,
        ))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(
            read_primitive::<f32>(data, validity, len)?,
        ))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(
            read_primitive::<f64>(data, validity, len)?,
        ))),
        DataType::Timestamp(unit, tz) => {
            // DuckDB timestamps are i64 microseconds (or per-unit); build the
            // matching Arrow timestamp array.
            use arrow::array::{
                TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
                TimestampSecondArray,
            };
            let values = read_primitive::<i64>(data, validity, len)?;
            let tz = tz.clone();
            match unit {
                TimeUnit::Second => Ok(Arc::new(TimestampSecondArray::from(values).with_timezone_opt(tz))),
                TimeUnit::Millisecond => {
                    Ok(Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(tz)))
                }
                TimeUnit::Microsecond => {
                    Ok(Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(tz)))
                }
                TimeUnit::Nanosecond => {
                    Ok(Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(tz)))
                }
            }
        }
        DataType::Date32 => {
            let values = read_primitive::<i32>(data, validity, len)?;
            Ok(Arc::new(arrow::array::Date32Array::from(values)))
        }
        DataType::Time64(_) => {
            let values = read_primitive::<i64>(data, validity, len)?;
            Ok(Arc::new(arrow::array::Time64MicrosecondArray::from(values)))
        }
        DataType::Utf8 => {
            let mut builder = arrow::array::StringBuilder::new();
            let strings = std::slice::from_raw_parts(data as *const duckdb_string_t, len);
            for i in 0..len {
                if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                    let mut s = strings[i];
                    let s_len = ffi::duckdb_string_t_length(s);
                    let s_ptr = ffi::duckdb_string_t_data(&mut s);
                    let bytes = std::slice::from_raw_parts(s_ptr as *const u8, s_len as usize);
                    builder.append_value(String::from_utf8_lossy(bytes));
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = arrow::array::BinaryBuilder::new();
            let blobs = std::slice::from_raw_parts(data as *const duckdb_string_t, len);
            for i in 0..len {
                if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                    let mut b = blobs[i];
                    let b_len = ffi::duckdb_string_t_length(b);
                    let b_ptr = ffi::duckdb_string_t_data(&mut b);
                    let bytes = std::slice::from_raw_parts(b_ptr as *const u8, b_len as usize);
                    builder.append_value(bytes);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::List(field) => {
            let entries = std::slice::from_raw_parts(data as *const ffi::duckdb_list_entry, len);
            let child = ffi::duckdb_list_vector_get_child(vector);
            let child_len = ffi::duckdb_list_vector_get_size(vector) as usize;
            let child_array = vector_to_arrow_array(child, child_len, field.data_type())?;
            let mut offsets: Vec<i32> = Vec::with_capacity(len + 1);
            let mut offset: i32 = 0;
            offsets.push(offset);
            for i in 0..len {
                if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                    let entry = entries[i];
                    offset = offset
                        .checked_add(entry.length as i32)
                        .ok_or("write_orc: list offset overflow")?;
                }
                offsets.push(offset);
            }
            let offsets = arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::from(offsets));
            let nulls = validity_to_null_buffer(validity, len);
            let arr = ListArray::try_new(field.clone(), offsets, child_array, nulls)
                .map_err(|e| format!("write_orc: failed to build ListArray: {e}"))?;
            Ok(Arc::new(arr))
        }
        DataType::Struct(fields) => {
            let child_count = ffi::duckdb_struct_type_child_count(ffi::duckdb_vector_get_column_type(vector));
            let mut child_arrays: Vec<ArrayRef> = Vec::with_capacity(fields.len());
            for i in 0..child_count as usize {
                let child = ffi::duckdb_struct_vector_get_child(vector, i as idx_t);
                let child_array = vector_to_arrow_array(child, len, fields[i].data_type())?;
                child_arrays.push(child_array);
            }
            let nulls = validity_to_null_buffer(validity, len);
            let arr = StructArray::try_new(
                fields.clone().into(),
                child_arrays,
                nulls,
            )
            .map_err(|e| format!("write_orc: failed to build StructArray: {e}"))?;
            Ok(Arc::new(arr))
        }
        DataType::Decimal128(_, _) => {
            // DuckDB DECIMAL physical storage: i16/i32/i64/i128 depending on width.
            let mut logical = ffi::duckdb_vector_get_column_type(vector);
            let width = ffi::duckdb_decimal_width(logical);
            ffi::duckdb_destroy_logical_type(&mut logical);
            let mut builder = arrow::array::Decimal128Builder::new();
            match width {
                0..=4 => {
                    let slice = std::slice::from_raw_parts(data as *const i16, len);
                    for i in 0..len {
                        if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                            builder.append_value(slice[i] as i128);
                        } else {
                            builder.append_null();
                        }
                    }
                }
                5..=9 => {
                    let slice = std::slice::from_raw_parts(data as *const i32, len);
                    for i in 0..len {
                        if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                            builder.append_value(slice[i] as i128);
                        } else {
                            builder.append_null();
                        }
                    }
                }
                10..=18 => {
                    let slice = std::slice::from_raw_parts(data as *const i64, len);
                    for i in 0..len {
                        if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                            builder.append_value(slice[i] as i128);
                        } else {
                            builder.append_null();
                        }
                    }
                }
                _ => {
                    let slice = std::slice::from_raw_parts(data as *const i128, len);
                    for i in 0..len {
                        if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
                            builder.append_value(slice[i]);
                        } else {
                            builder.append_null();
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        other => Err(format!("write_orc: unsupported Arrow type {other:?}").into()),
    }
}

/// Read a primitive column into `Vec<Option<T>>`, honoring the validity mask.
unsafe fn read_primitive<T: Copy>(
    data: *mut std::ffi::c_void,
    validity: *mut u64,
    len: usize,
) -> Result<Vec<Option<T>>, Box<dyn Error>> {
    let slice = std::slice::from_raw_parts(data as *const T, len);
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        if validity.is_null() || ffi::duckdb_validity_row_is_valid(validity, i as idx_t) {
            out.push(Some(slice[i]));
        } else {
            out.push(None);
        }
    }
    Ok(out)
}

/// Convert a DuckDB validity mask into an Arrow `NullBuffer`.
unsafe fn validity_to_null_buffer(validity: *mut u64, len: usize) -> Option<arrow::buffer::NullBuffer> {
    if validity.is_null() {
        return None;
    }
    let mut valid = Vec::with_capacity(len);
    for i in 0..len {
        valid.push(ffi::duckdb_validity_row_is_valid(validity, i as idx_t));
    }
    Some(arrow::buffer::NullBuffer::from(valid))
}

// ---------------------------------------------------------------------------
// COPY function callbacks
// ---------------------------------------------------------------------------

unsafe extern "C" fn copy_bind(info: duckdb_copy_function_bind_info) {
    if let Err(e) = do_bind(info) {
        let msg = CString::new(e.to_string()).unwrap_or_default();
        ffi::duckdb_copy_function_bind_set_error(info, msg.as_ptr());
    }
}

unsafe fn do_bind(info: duckdb_copy_function_bind_info) -> Result<(), Box<dyn Error>> {
    let ncols = ffi::duckdb_copy_function_bind_get_column_count(info);
    let mut fields: Vec<Field> = Vec::with_capacity(ncols as usize);
    for i in 0..ncols {
        let mut logical_type = ffi::duckdb_copy_function_bind_get_column_type(info, i);
        if logical_type.is_null() {
            return Err(format!("write_orc: bind get_column_type({i}) returned NULL").into());
        }
        let data_type = logical_type_to_arrow(logical_type)?;
        // Try to get the column name from the logical type alias.
        let alias = ffi::duckdb_logical_type_get_alias(logical_type);
        let name = if alias.is_null() {
            format!("column_{i}")
        } else {
            let s = CStr::from_ptr(alias).to_string_lossy().into_owned();
            ffi::duckdb_free(alias as *mut _);
            s
        };
        ffi::duckdb_destroy_logical_type(&mut logical_type);
        fields.push(Field::new(name, data_type, true));
    }

    let schema: SchemaRef = Arc::new(Schema::new(fields));
    validate_schema_for_write(&schema)?;
    let bind_data = Box::new(OrcCopyBindData { schema });
    ffi::duckdb_copy_function_bind_set_bind_data(info, Box::into_raw(bind_data) as *mut _, Some(destroy_bind_data));
    Ok(())
}

unsafe extern "C" fn destroy_bind_data(data: *mut std::ffi::c_void) {
    if !data.is_null() {
        drop(Box::from_raw(data as *mut OrcCopyBindData));
    }
}

unsafe extern "C" fn copy_global_init(info: duckdb_copy_function_global_init_info) {
    if let Err(e) = do_global_init(info) {
        let msg = CString::new(e.to_string()).unwrap_or_default();
        ffi::duckdb_copy_function_global_init_set_error(info, msg.as_ptr());
    }
}

unsafe fn do_global_init(info: duckdb_copy_function_global_init_info) -> Result<(), Box<dyn Error>> {
    let path_ptr = ffi::duckdb_copy_function_global_init_get_file_path(info);
    if path_ptr.is_null() {
        return Err("write_orc: COPY TO path is NULL".into());
    }
    let path = CStr::from_ptr(path_ptr).to_string_lossy().into_owned();

    let bind_data = ffi::duckdb_copy_function_global_init_get_bind_data(info);
    if bind_data.is_null() {
        return Err("write_orc: bind data is NULL".into());
    }
    let bind: &OrcCopyBindData = &*(bind_data as *const OrcCopyBindData);
    let schema = bind.schema.clone();

    let file = File::create(&path)
        .map_err(|e| format!("write_orc: cannot create '{path}': {e}"))?;
    let writer = ArrowWriterBuilder::new(file, schema)
        .with_batch_size(2048)
        .with_compression(CompressionType::Zstd)
        .try_build()
        .map_err(|e| format!("write_orc: failed to create ORC writer for '{path}': {e}"))?;

    let state = Box::new(OrcCopyState { writer: Some(writer) });
    ffi::duckdb_copy_function_global_init_set_global_state(info, Box::into_raw(state) as *mut _, Some(destroy_state));
    Ok(())
}

unsafe extern "C" fn destroy_state(data: *mut std::ffi::c_void) {
    if !data.is_null() {
        drop(Box::from_raw(data as *mut OrcCopyState));
    }
}

unsafe extern "C" fn copy_sink(info: duckdb_copy_function_sink_info, input: duckdb_data_chunk) {
    if let Err(e) = do_sink(info, input) {
        let msg = CString::new(e.to_string()).unwrap_or_default();
        ffi::duckdb_copy_function_sink_set_error(info, msg.as_ptr());
    }
}

unsafe fn do_sink(info: duckdb_copy_function_sink_info, input: duckdb_data_chunk) -> Result<(), Box<dyn Error>> {
    let state_ptr = ffi::duckdb_copy_function_sink_get_global_state(info);
    if state_ptr.is_null() {
        return Err("write_orc: global state is NULL in sink".into());
    }
    let bind_ptr = ffi::duckdb_copy_function_sink_get_bind_data(info);
    if bind_ptr.is_null() {
        return Err("write_orc: bind data is NULL in sink".into());
    }
    let state: &mut OrcCopyState = &mut *(state_ptr as *mut OrcCopyState);
    let bind: &OrcCopyBindData = &*(bind_ptr as *const OrcCopyBindData);

    let batch = chunk_to_record_batch(input, &bind.schema)?;
    let writer = state
        .writer
        .as_mut()
        .ok_or("write_orc: writer already closed")?;
    writer
        .write(&batch)
        .map_err(|e| format!("write_orc: failed to write batch: {e}"))?;
    Ok(())
}

unsafe extern "C" fn copy_finalize(info: duckdb_copy_function_finalize_info) {
    let state_ptr = ffi::duckdb_copy_function_finalize_get_global_state(info);
    if state_ptr.is_null() {
        let msg = c"write_orc: global state is NULL in finalize";
        ffi::duckdb_copy_function_finalize_set_error(info, msg.as_ptr());
        return;
    }
    let state: &mut OrcCopyState = &mut *(state_ptr as *mut OrcCopyState);
    match state.writer.take() {
        Some(writer) => {
            if let Err(e) = writer.close() {
                let msg = CString::new(format!("write_orc: failed to close writer: {e}"))
                    .unwrap_or_default();
                ffi::duckdb_copy_function_finalize_set_error(info, msg.as_ptr());
            }
        }
        None => {
            let msg = c"write_orc: writer already closed";
            ffi::duckdb_copy_function_finalize_set_error(info, msg.as_ptr());
        }
    }
}

/// Create and register the `orc` COPY function on the given connection.
///
/// # Safety
/// `con` must be a valid `duckdb_connection`.
pub unsafe fn register_copy_function(con: duckdb::ffi::duckdb_connection) -> Result<(), Box<dyn Error>> {
    let copy_function: duckdb_copy_function = ffi::duckdb_create_copy_function();
    if copy_function.is_null() {
        return Err("write_orc: duckdb_create_copy_function returned NULL".into());
    }

    let name = CString::new("orc").unwrap();
    ffi::duckdb_copy_function_set_name(copy_function, name.as_ptr());
    ffi::duckdb_copy_function_set_bind(copy_function, Some(copy_bind));
    ffi::duckdb_copy_function_set_global_init(copy_function, Some(copy_global_init));
    ffi::duckdb_copy_function_set_sink(copy_function, Some(copy_sink));
    ffi::duckdb_copy_function_set_finalize(copy_function, Some(copy_finalize));

    let rc = ffi::duckdb_register_copy_function(con, copy_function);
    // The copy function is now owned by DuckDB; the local handle must not be destroyed.
    if rc != ffi::duckdb_state_DuckDBSuccess {
        return Err("write_orc: duckdb_register_copy_function failed".into());
    }
    Ok(())
}

