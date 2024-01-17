mod writer;
mod read;

use writer::writer_parquet;
use read::read_parquet;



fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Definindo o esquema
    writer_parquet()?;

    read_parquet()?;

    return Ok( ());
}
