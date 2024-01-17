use arrow::array::{BooleanArray, Float64Array, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;

pub fn writer_parquet() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("nome", DataType::Utf8, false),
        Field::new("ativo", DataType::Boolean, false),
        Field::new("saldo", DataType::Float64, true),
    ]));

    // Criando dados de exemplo
    let id = Int32Array::from(vec![1, 2, 3]);
    let nome = StringArray::from(vec!["Alice", "Bob", "Carol"]);
    let ativo = BooleanArray::from(vec![true, false, true]);
    let saldo = Float64Array::from(vec![Some(100.0), None, Some(150.0)]);

    // Criando um RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id) as Arc<dyn arrow::array::Array>,
            Arc::new(nome),
            Arc::new(ativo),
            Arc::new(saldo),
        ],
    )?;

    let file = File::create("example.parquet")?;

    // Escrevendo os dados em um arquivo Parquet
    // let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    // Assuming `to_write` is a RecordBatch
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}