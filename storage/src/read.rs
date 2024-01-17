
use std::fs::File;

use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use prettytable::{Table, Row, Cell};
use arrow::array::{Array, StringArray, Int32Array}; // Impor

// Print on format table
/*
Example:
+----+-------+-------+-------+
| id | nome  | ativo | saldo |
+----+-------+-------+-------+
| 1  | Alice | true  | 100.0 |
+----+-------+-------+-------+
| 2  | Bob   | false | null  |
+----+-------+-------+-------+
 */
fn format_and_print_parquet(batches: &[RecordBatch]) {
    for batch in batches {
        let mut table = Table::new();

        // Adicionando cabeçalho
        let headers: Vec<Cell> = batch
            .schema()
            .fields()
            .iter()
            .map(|field| Cell::new(field.name()))
            .collect();
        table.add_row(Row::new(headers));

        // Adicionando linhas de dados
        for row in 0..batch.num_rows() {
            let row_data: Vec<Cell> = (0..batch.num_columns())
                .map(|col| {
                    let col_data = batch.column(col);
                    match col_data.data_type() {
                        // Exemplo para String e Int32 - expanda conforme necessário
                        &DataType::Utf8 => {
                            let array = col_data.as_any().downcast_ref::<StringArray>().unwrap();
                            Cell::new(array.value(row))
                        }
                        &DataType::Int32 => {
                            let array = col_data.as_any().downcast_ref::<Int32Array>().unwrap();
                            Cell::new(&array.value(row).to_string())
                        },
                        &DataType::Float64 => {
                            let array = col_data.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
                            Cell::new(&array.value(row).to_string())
                        },
                        &DataType::Boolean => {
                            let array = col_data.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
                            Cell::new(&array.value(row).to_string())
                        },
                        &DataType::Date32 => {
                            let array = col_data.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
                            Cell::new(&array.value(row).to_string())
                        },
                        // Outros tipos...
                        _ => Cell::new("Unsupported"),
                    }
                })
                .collect();
            table.add_row(Row::new(row_data));
        }

        // Imprime a tabela formatada
        table.printstd();
    }
}


pub fn read_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open("example.parquet")?;

    // Create a sync parquet reader with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;

    let mut batches: Vec<arrow::record_batch::RecordBatch> = Vec::new();

    for batch in parquet_reader {
        batches.push(batch?);
    }

    format_and_print_parquet(&batches);
    
    Ok(())
}