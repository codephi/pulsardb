use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

use crate::valuetable::ValueTable;

pub fn read_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let file = File::open("example.parquet")?;

    // Create a sync parquet reader with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(8192)
        .build()?;

    let mut batches: Vec<RecordBatch> = Vec::new();

    for batch in parquet_reader {
        batches.push(batch?);
    }
    
    let table = ValueTable::from(&batches);

    table.print_table();

    Ok(())
}
