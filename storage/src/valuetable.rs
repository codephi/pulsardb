use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;
use std::vec;
use valu3::prelude::*;

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, StringArray};
use prettytable::{Cell, Row, Table};

#[derive(Debug, Clone)]
pub struct ValueTable {
    headers: Vec<String>,
    cols: Vec<Vec<Value>>,
}

impl ValueTable {
    pub fn new() -> Self {
        Self {
            headers: vec![],
            cols: vec![],
        }
    }

    pub fn count_rows(&self) -> usize {
        self.cols.get(0).unwrap().len()
    }

    pub fn add_col(&mut self, col: Vec<Value>) {
        self.cols.push(col);
    }

    pub fn push_item(&mut self, col: usize, value: Value) {
        self.cols[col].push(value);
    }

    pub fn push_item_in_header(&mut self, header: &str, value: Value) {
        let index = self.headers.iter().position(|x| x == header);

        if let Some(index) = index {
            self.push_item(index, value);
        } else {
            let index = self.add_header(header.to_string());
            self.push_item(index, value);
        }
    }

    pub fn add_header(&mut self, header: String) -> usize {
        self.headers.push(header);
        self.headers.len() - 1
    }

    pub fn add_value(&mut self, col: usize, row: usize, value: Value) {
        self.cols[col][row] = value;
    }

    pub fn get_header(&self, index: usize) -> Option<&String> {
        self.headers.get(index)
    }

    pub fn get_headers(&self) -> &Vec<String> {
        &self.headers
    }

    pub fn get_value(&self, col: usize, row: usize) -> Option<&Value> {
        self.cols.get(col)?.get(row)
    }

    pub fn print_table(&self) {
        let mut table = Table::new();

        table.add_row(Row::new(
            self.headers.iter().map(|x| Cell::new(x)).collect(),
        ));

        for col in &self.cols {
            table.add_row(Row::new(
                col.iter().map(|x| Cell::new(&x.to_string())).collect(),
            ));
        }

        table.printstd();
    }

    pub fn to_map(&self) -> Vec<HashMap<String, Value>> {
        let mut map: Vec<HashMap<String, Value>> = vec![];

        for col in &self.cols {
            let mut col_map: HashMap<String, Value> = HashMap::new();

            let header: &String = self
                .headers
                .get(self.cols.iter().position(|x| x == col).unwrap())
                .unwrap();

            for value in col.iter() {
                col_map.insert(header.clone(), value.clone());
            }

            map.push(col_map);
        }

        map
    }

    pub fn to_value(&self) -> Value {
        Value::from(self.to_map())
    }

    pub fn to_json(&self) -> String {
        self.to_value().to_string()
    }
}

macro_rules! array_to_value {
    ($array:ident, $row:ident) => {
        if $array.is_nullable() && $array.is_null($row) {
            Value::Null
        } else {
            Value::from($array.value($row))
        }
    };
}

impl From<&RecordBatch> for ValueTable {
    fn from(batch: &RecordBatch) -> Self {
        let mut table = Self::new();

        for col in 0..batch.num_columns() {
            let col_data: &Arc<dyn Array> = batch.column(col);
            let new_col_data = (0..batch.num_rows())
                .map(|row| match col_data.data_type() {
                    &DataType::Null => Value::Null,
                    &DataType::Boolean => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Int8 => {
                        let array = col_data.as_any().downcast_ref::<Int32Array>().unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Int16 => {
                        let array = col_data.as_any().downcast_ref::<Int32Array>().unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Int32 => {
                        let array = col_data.as_any().downcast_ref::<Int32Array>().unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Int64 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::Int64Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::UInt8 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::UInt8Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::UInt16 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::UInt16Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::UInt32 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::UInt32Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::UInt64 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::UInt64Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Float32 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::Float32Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Float64 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Timestamp(_, _) => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                            .unwrap();

                        if array.is_nullable() && array.is_null(row) {
                            Value::Null
                        } else {
                            let value = array.value(row);
                            DateTime::from(value).to_value()
                        }
                    }
                    &DataType::Date32 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::Date32Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Date64 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::Date64Array>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Time32(_) => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::Time32SecondArray>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Time64(_) => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::Time64NanosecondArray>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Duration(_) => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::DurationNanosecondArray>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Interval(_) => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::IntervalMonthDayNanoArray>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Utf8 => {
                        let array = col_data.as_any().downcast_ref::<StringArray>().unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::LargeUtf8 => {
                        let array = col_data
                            .as_any()
                            .downcast_ref::<arrow::array::LargeStringArray>()
                            .unwrap();
                        array_to_value!(array, row)
                    }
                    &DataType::Decimal128(_, _) => {
                        let array: &arrow::array::PrimitiveArray<arrow::datatypes::Decimal128Type> =
                            col_data
                                .as_any()
                                .downcast_ref::<arrow::array::Decimal128Array>()
                                .unwrap();
                        array_to_value!(array, row)
                    }
                    _ => Value::Null,
                })
                .collect::<Vec<_>>();

            table.add_col(new_col_data);
        }

        for col in 0..batch.num_columns() {
            table.add_header(batch.schema().field(col).name().clone());
        }

        table
    }
}

impl From<&Vec<RecordBatch>> for ValueTable {
    fn from(batches: &Vec<RecordBatch>) -> Self {
        let mut table = Self::new();

        for batch in batches {
            let mut batch_table = ValueTable::from(batch);

            table.headers.append(&mut batch_table.headers);
            table.cols.append(&mut batch_table.cols);
        }

        table
    }
}

#[derive(Debug)]
pub enum ValueTableError {
    InvalidDataType(String),
    CreateRecordBatch(String),
}

macro_rules! value_get_i32 {
    ($value:expr) => {
        match $value.get_i32() {
            Some(value) => value,
            None => match $value.get_f64() {
                Some(value) => value as i32,
                None => {
                    return Err(ValueTableError::InvalidDataType(
                        format!(
                            "A list that starts with i32 can only work with i32 or float64. Current value: {}",
                            $value
                        ),
                    ))
                }
            },
        }
    };
}

macro_rules! value_get_f64 {
    ($value:expr) => {
        match $value.get_f64() {
            Some(value) => value,
            None => match $value.get_i32() {
                Some(value) => value as f64,
                None => {
                    return Err(ValueTableError::InvalidDataType(
                        format!(
                            "A list that starts with float64 can only work with f64 or i32. Current value: {}",
                            $value
                        ),
                    ))
                }
            },
        }
    };
}

impl TryFrom<&ValueTable> for RecordBatch {
    type Error = ValueTableError;

    fn try_from(value_table: &ValueTable) -> Result<Self, Self::Error> {
        let mut schema_types: Vec<DataType> = Vec::new();

        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        macro_rules! columns_push {
            ($col_is_nullable:expr, $values:expr,$type:ident) => {
                if $col_is_nullable {
                    columns.push(Arc::new($type::from($values)));
                } else {
                    columns.push(Arc::new($type::from(
                        $values
                            .iter()
                            .map(|x| x.clone().unwrap())
                            .collect::<Vec<_>>(),
                    )));
                }
            };
        }

        for col in value_table.cols.iter() {
            let mut col_iter = col.iter();
            let mut total_prepend_nulls = 0;

            while let Some(item) = col_iter.next() {
                match item {
                    Value::String(value) => {
                        schema_types.push(DataType::Utf8);

                        let mut col_is_nullable = total_prepend_nulls > 0;

                        let mut values = vec![None; total_prepend_nulls];
                        values.push(Some(value.to_string()));

                        while let Some(value) = col_iter.next() {
                            match &value {
                                Value::String(value) => {
                                    values.push(Some(value.to_string()));
                                }
                                _ => {
                                    if !col_is_nullable {
                                        col_is_nullable = true;
                                    }

                                    values.push(None)
                                }
                            }
                        }

                        columns_push!(col_is_nullable, values, StringArray);

                        break;
                    }
                    Value::Number(value) => {
                        if value.is_float() {
                            schema_types.push(DataType::Float64);

                            let mut col_is_nullable = total_prepend_nulls > 0;

                            let mut values = vec![None; total_prepend_nulls];
                            values.push(Some(value_get_f64!(value)));

                            while let Some(value) = col_iter.next() {
                                match &value {
                                    Value::Number(value) => {
                                        values.push(Some(value_get_f64!(value)));
                                    }
                                    _ => {
                                        if !col_is_nullable {
                                            col_is_nullable = true;
                                        }

                                        values.push(None)
                                    }
                                }
                            }

                            columns_push!(col_is_nullable, values, Float64Array);

                            break;
                        } else {
                            schema_types.push(DataType::Int32);

                            let mut col_is_nullable = total_prepend_nulls > 0;

                            let mut values = vec![None; total_prepend_nulls];
                            values.push(Some(value_get_i32!(value)));

                            while let Some(value) = col_iter.next() {
                                match &value {
                                    Value::Number(value) => {
                                        values.push(Some(value_get_i32!(value)));
                                    }
                                    _ => {
                                        if !col_is_nullable {
                                            col_is_nullable = true;
                                        }

                                        values.push(None)
                                    }
                                }
                            }

                            columns_push!(col_is_nullable, values, Int32Array);

                            break;
                        }
                    }
                    Value::Boolean(value) => {
                        schema_types.push(DataType::Boolean);

                        let mut col_is_nullable = total_prepend_nulls > 0;

                        let mut values = vec![None; total_prepend_nulls];
                        values.push(Some(*value));

                        while let Some(value) = col_iter.next() {
                            match &value {
                                Value::Boolean(value) => {
                                    values.push(Some(*value));
                                }
                                _ => {
                                    if !col_is_nullable {
                                        col_is_nullable = true;
                                    }

                                    values.push(None)
                                }
                            }
                        }

                        columns_push!(col_is_nullable, values, BooleanArray);

                        break;
                    }
                    Value::Object(_) | Value::Array(_) => {
                        schema_types.push(DataType::Utf8);

                        let mut col_is_nullable = total_prepend_nulls > 0;

                        let mut values = vec![None; total_prepend_nulls];
                        values.push(Some(item.to_json(JsonMode::Inline)));

                        while let Some(value) = col_iter.next() {
                            match &value {
                                Value::Object(_) => {
                                    values.push(Some(value.to_json(JsonMode::Inline)));
                                }
                                _ => {
                                    if !col_is_nullable {
                                        col_is_nullable = true;
                                    }

                                    values.push(None)
                                }
                            }
                        }

                        columns_push!(col_is_nullable, values, StringArray);

                        break;
                    }
                    _ => {
                        total_prepend_nulls += 1;
                    }
                };
            }
        }

        let schema = {
            let fields = value_table
                .headers
                .iter()
                .enumerate()
                .map(|(index, header)| Field::new(header, schema_types[index].clone(), true))
                .collect::<Vec<_>>();

            Arc::new(Schema::new(fields))
        };

        Ok(match RecordBatch::try_new(schema, columns) {
            Ok(record_batch) => record_batch,
            Err(error) => {
                return Err(ValueTableError::CreateRecordBatch(format!(
                    "Error creating record batch: {}",
                    error
                )))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_table() {
        let mut table = ValueTable::new();

        table.add_header("id".to_string());
        table.add_header("name".to_string());
        table.add_header("active".to_string());
        table.add_header("amount".to_string());

        table.add_col(vec![Value::from(1), Value::from(2), Value::from(3)]);
        table.add_col(vec![
            Value::from(Some("Bob")),
            Value::from(None::<Option<String>>),
            Value::from(Some("Carol")),
        ]);
        table.add_col(vec![
            Value::from(true),
            Value::from(false),
            Value::from(true),
        ]);
        table.add_col(vec![Value::from(100.0), Value::Null, Value::from(150.0)]);

        let batch = RecordBatch::try_from(&table);

        let new_table = ValueTable::from(&batch.unwrap());

        assert_eq!(table.headers, new_table.headers);
        assert!(table
            .cols
            .iter()
            .zip(new_table.cols.iter())
            .all(|(a, b)| a == b));
    }
}
