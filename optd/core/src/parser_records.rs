use anyhow::{anyhow, bail, Result};
use datafusion::arrow::array::{
    Array, BooleanArray, Float16Array, Float32Array, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, RecordBatch, StringArray, StringViewArray, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use std::str::FromStr;

use crate::ir::DataType;

pub fn parse_data_type(value: &str) -> Option<DataType> {
    DataType::from_str(value.trim()).ok()
}

pub fn parse_float(batch: &RecordBatch, row: usize, column: &str, allow_missing_column: bool) -> Result<Option<f32>> {
    let Some(col) = batch.column_by_name(column) else {
        return if allow_missing_column {
            Ok(None)
        } else {
            Err(anyhow!("missing '{}' column", column))
        };
    };

    let col = col.as_any();

    if let Some(col) = col.downcast_ref::<Float16Array>() {
        return Ok((!col.is_null(row)).then(|| f32::from(col.value(row))));
    }

    if let Some(col) = col.downcast_ref::<Float32Array>() {
        return Ok((!col.is_null(row)).then(|| col.value(row)));
    }

    if let Some(col) = col.downcast_ref::<Float64Array>() {
        return Ok((!col.is_null(row)).then(|| col.value(row) as f32));
    }

    bail!("column '{}' is not a floating-point array", column)
}

pub fn parse_int(batch: &RecordBatch, row: usize, column: &str) -> Result<i64> {
    let col = batch
        .column_by_name(column)
        .ok_or_else(|| anyhow!("missing '{}' column", column))?
        .as_any();

    macro_rules! parse_integer_array {
        ($array_type:ty) => {
            if let Some(col) = col.downcast_ref::<$array_type>() {
                if col.is_null(row) {
                    bail!("column '{}' is NULL at row {}", column, row);
                }
                return Ok(i64::from(col.value(row)));
            }
        };
    }

    parse_integer_array!(Int8Array);
    parse_integer_array!(Int16Array);
    parse_integer_array!(Int32Array);
    parse_integer_array!(Int64Array);
    parse_integer_array!(UInt8Array);
    parse_integer_array!(UInt16Array);
    parse_integer_array!(UInt32Array);

    if let Some(col) = col.downcast_ref::<UInt64Array>() {
        if col.is_null(row) {
            bail!("column '{}' is NULL at row {}", column, row);
        }

        return i64::try_from(col.value(row))
            .map_err(|_| anyhow!("column '{}' value at row {} does not fit in i64", column, row));
    }

    bail!("column '{}' is not an integer array", column)
}

pub fn parse_string(batch: &RecordBatch, row: usize, column: &str) -> Result<String> {
    let arr = batch
        .column_by_name(column)
        .ok_or_else(|| anyhow!("missing '{}' column", column))?;

    if let Some(col) = arr.as_any().downcast_ref::<StringViewArray>() {
        if col.is_null(row) {
            bail!("column '{}' is NULL at row {}", column, row);
        }
        return Ok(col.value(row).to_string());
    }

    if let Some(col) = arr.as_any().downcast_ref::<StringArray>() {
        if col.is_null(row) {
            bail!("column '{}' is NULL at row {}", column, row);
        }
        return Ok(col.value(row).to_string());
    }

    bail!("column '{}' is not Utf8/Utf8View", column)
}

pub fn parse_nullable_string(batch: &RecordBatch, row: usize, column: &str) -> Result<Option<String>> {
    let arr = batch
        .column_by_name(column)
        .ok_or_else(|| anyhow!("missing '{}' column", column))?;

    if let Some(col) = arr.as_any().downcast_ref::<StringViewArray>() {
        return if col.is_null(row) {
            Ok(None)
        } else {
            Ok(Some(col.value(row).to_string()))
        };
    }

    if let Some(col) = arr.as_any().downcast_ref::<StringArray>() {
        return if col.is_null(row) {
            Ok(None)
        } else {
            Ok(Some(col.value(row).to_string()))
        };
    }

    bail!("column '{}' is not Utf8/Utf8View", column)
}

pub fn parse_bool(batch: &RecordBatch, row: usize, column: &str) -> Result<bool> {
    let arr = batch
        .column_by_name(column)
        .ok_or_else(|| anyhow!("missing '{}' column", column))?;

    let Some(col) = arr.as_any().downcast_ref::<BooleanArray>() else {
        bail!("column '{}' is not Boolean", column)
    };
    if col.is_null(row) {
        bail!("column '{}' is NULL at row {}", column, row);
    }
    Ok(col.value(row))
}