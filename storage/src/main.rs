mod writer;
mod read;
mod valuetable;

use writer::writer_parquet;
use read::read_parquet;



fn main() -> Result<(), Box<dyn std::error::Error>> {
    writer_parquet()?;

    read_parquet()?;

    return Ok( ());
}
