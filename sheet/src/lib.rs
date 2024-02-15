use byteorder::ReadBytesExt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Cursor, Read, Write};

const DATA_TYPE_UNDEFINED: u8 = 0;
const DATA_TYPE_NULL: u8 = 1;
const DATA_TYPE_BOOLEAN: u8 = 2;
const DATA_TYPE_VARCHAR: u8 = 3;
const DATA_TYPE_TEXT: u8 = 4;
const DATA_TYPE_INTEGER_8: u8 = 5;
const DATA_TYPE_INTEGER_16: u8 = 6;
const DATA_TYPE_INTEGER_32: u8 = 7;
const DATA_TYPE_INTEGER_64: u8 = 8;
const DATA_TYPE_INTEGER_128: u8 = 9;
const DATA_TYPE_FLOAT_32: u8 = 10;
const DATA_TYPE_FLOAT_64: u8 = 11;

const NULL_BIN_VALUE: u8 = 0;
const FALSE_BIN_VALUE: u8 = 0;
const TRUE_BIN_VALUE: u8 = 1;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    WriteInvalidDataType,
    ReadInvalidDataType,
    VarcharSize,
}

// resolve!(File::create("file.bin"), Error::Io);
// compile:
// match File::create("file.bin") {
//     Ok(file) => file,
//     Err(e) => return Err(Error::Io(e)),
// }
/// Macro to handle Result and return a custom error
/// # Example
/// ```
/// let file = th!(File::create("file.bin"), Error::Io);
/// ```
/// # Expands to
/// ```
/// match File::create("file.bin") {
///    Ok(file) => file,
///   Err(e) => return Err(Error::Io(e)),
/// }
/// ```
macro_rules! th {
    ($result:expr, $error:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => return Err($error(e)),
        }
    };
}

#[derive(Debug, PartialEq, Clone)]
enum DataType {
    Null,
    Boolean,
    Text,
    Varchar(u32),
    Integer8,
    Integer16,
    Integer32,
    Integer64,
    Integer128,
    Float32,
    Float64,
    Undefined,
}

impl Into<u8> for DataType {
    fn into(self) -> u8 {
        match self {
            DataType::Null => DATA_TYPE_NULL,
            DataType::Boolean => DATA_TYPE_BOOLEAN,
            DataType::Text => DATA_TYPE_TEXT,
            DataType::Varchar(_) => DATA_TYPE_VARCHAR,
            DataType::Integer8 => DATA_TYPE_INTEGER_8,
            DataType::Integer16 => DATA_TYPE_INTEGER_16,
            DataType::Integer32 => DATA_TYPE_INTEGER_32,
            DataType::Integer64 => DATA_TYPE_INTEGER_64,
            DataType::Integer128 => DATA_TYPE_INTEGER_128,
            DataType::Float32 => DATA_TYPE_FLOAT_32,
            DataType::Float64 => DATA_TYPE_FLOAT_64,
            DataType::Undefined => DATA_TYPE_UNDEFINED,
        }
    }
}

impl From<u8> for DataType {
    fn from(byte: u8) -> Self {
        match byte {
            DATA_TYPE_NULL => DataType::Null,
            DATA_TYPE_BOOLEAN => DataType::Boolean,
            DATA_TYPE_TEXT => DataType::Text,
            DATA_TYPE_VARCHAR => DataType::Varchar(0),
            DATA_TYPE_INTEGER_8 => DataType::Integer8,
            DATA_TYPE_INTEGER_16 => DataType::Integer16,
            DATA_TYPE_INTEGER_32 => DataType::Integer32,
            DATA_TYPE_INTEGER_64 => DataType::Integer64,
            DATA_TYPE_INTEGER_128 => DataType::Integer128,
            DATA_TYPE_FLOAT_32 => DataType::Float32,
            DATA_TYPE_FLOAT_64 => DataType::Float64,
            _ => DataType::Undefined,
        }
    }
}

#[derive(Debug, PartialEq)]
struct Property {
    data_type: DataType,
    label: String,
}

impl Property {
    pub fn size(&self) -> usize {
        match self.data_type {
            DataType::Varchar(size) => size as usize,
            DataType::Boolean => 1,
            DataType::Text => 0,
            DataType::Null => 0,
            DataType::Integer8 => 1,
            DataType::Integer16 => 2,
            DataType::Integer32 => 4,
            DataType::Integer64 => 8,
            DataType::Integer128 => 16,
            DataType::Float32 => 4,
            DataType::Float64 => 8,
            _ => 0,
        }
    }
}

pub type Header = Vec<Property>;

fn write_header(path: &str, properties: &Header) -> Result<(), Error> {
    let mut file = BufWriter::new(th!(File::create(path), Error::Io));

    for prop in properties {
        let data_type_byte = prop.data_type.clone().into();
        let label_bytes = prop.label.as_bytes();
        // Write data type
        th!(file.write_all(&[data_type_byte]), Error::Io);

        if let DataType::Varchar(size) = prop.data_type {
            th!(file.write_all(&size.to_le_bytes()), Error::Io);
        }

        // Write label size
        th!(
            file.write_all(&(label_bytes.len() as u32).to_le_bytes()),
            Error::Io
        );
        // Write label
        th!(file.write_all(label_bytes), Error::Io);
    }

    th!(file.flush(), Error::Io);
    Ok(())
}

fn read_header(path: &str) -> Result<Header, Error> {
    let file = th!(File::open(path), Error::Io);
    let mut reader = BufReader::new(file);
    let mut properties = Vec::new();

    while let Ok(data_type_byte) = reader.read_u8() {
        let data_type = {
            if data_type_byte == DATA_TYPE_UNDEFINED {
                return Err(Error::ReadInvalidDataType);
            }

            if data_type_byte == DATA_TYPE_VARCHAR {
                let size = th!(reader.read_u32::<byteorder::LittleEndian>(), Error::Io);
                DataType::Varchar(size)
            } else {
                DataType::from(data_type_byte)
            }
        };

        let label_size = th!(reader.read_u32::<byteorder::LittleEndian>(), Error::Io);

        let mut label_bytes = vec![0u8; label_size as usize];

        th!(reader.read_exact(&mut label_bytes), Error::Io);

        let label = String::from_utf8(label_bytes).expect("UTF-8 decoding error");

        properties.push(Property { data_type, label });
    }

    Ok(properties)
}

#[derive(Debug, PartialEq)]
enum Value {
    String(String),
    Boolean(bool),
    Numeric(String),
    Null,
}

fn write_values(path: &str, header: &Header, values: &Vec<Value>) -> Result<(), Error> {
    let mut file = BufWriter::new(th!(File::create(path), Error::Io));

    for (index, value) in values.iter().enumerate() {
        match value {
            Value::Boolean(data) => {
                th!(
                    file.write_all(&[if *data {
                        TRUE_BIN_VALUE
                    } else {
                        FALSE_BIN_VALUE
                    }]),
                    Error::Io
                );
            }
            Value::String(data) => {
                let prop = &header[index];
                match prop.data_type {
                    DataType::Varchar(size) => {
                        if data.len() > size as usize {
                            return Err(Error::VarcharSize);
                        }

                        let mut buffer = vec![0u8; size as usize];
                        buffer[..data.len()].copy_from_slice(data.as_bytes());

                        th!(file.write_all(&buffer), Error::Io);
                    }
                    DataType::Text => {
                        th!(
                            file.write_all(&(data.len() as u32).to_le_bytes()),
                            Error::Io
                        );
                        th!(file.write_all(data.as_bytes()), Error::Io);
                    }
                    _ => return Err(Error::WriteInvalidDataType),
                }
            }
            Value::Numeric(data) => {
                let prop = &header[index];
                match prop.data_type {
                    DataType::Integer8 => {
                        th!(
                            file.write_all(&[data.parse::<i8>().unwrap() as u8]),
                            Error::Io
                        );
                    }
                    DataType::Integer16 => {
                        th!(
                            file.write_all(&data.parse::<i16>().unwrap().to_le_bytes()),
                            Error::Io
                        );
                    }
                    DataType::Integer32 => {
                        th!(
                            file.write_all(&data.parse::<i32>().unwrap().to_le_bytes()),
                            Error::Io
                        );
                    }
                    DataType::Integer64 => {
                        th!(
                            file.write_all(&data.parse::<i64>().unwrap().to_le_bytes()),
                            Error::Io
                        );
                    }
                    DataType::Integer128 => {
                        th!(
                            file.write_all(&data.parse::<i128>().unwrap().to_le_bytes()),
                            Error::Io
                        );
                    }
                    DataType::Float32 => {
                        th!(
                            file.write_all(&data.parse::<f32>().unwrap().to_le_bytes()),
                            Error::Io
                        );
                    }
                    DataType::Float64 => {
                        th!(
                            file.write_all(&data.parse::<f64>().unwrap().to_le_bytes()),
                            Error::Io
                        );
                    }
                    _ => return Err(Error::WriteInvalidDataType),
                }
            }
            Value::Null => {
                th!(file.write_all(&[NULL_BIN_VALUE]), Error::Io);
            }
        }
    }

    th!(file.flush(), Error::Io);
    Ok(())
}

pub fn read_values(path: &str, header: &Header) -> Result<Vec<Value>, Error> {
    let file = th!(File::open(path), Error::Io);
    let mut reader = BufReader::new(file);
    let mut values = Vec::new();

    for prop in header {
        let value = match prop.data_type {
            DataType::Varchar(size) => {
                let mut buffer = vec![0u8; size as usize];

                th!(reader.read_exact(&mut buffer), Error::Io);
                
                // remove null bytes
                let buffer = buffer
                    .iter()
                    .take_while(|&&b| b != 0u8)
                    .cloned()
                    .collect::<Vec<u8>>();

                Value::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Text => {
                let size = th!(reader.read_u32::<byteorder::LittleEndian>(), Error::Io) as usize;

                let mut buffer = vec![0; size];

                th!(reader.read_exact(&mut buffer), Error::Io);

                Value::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Boolean => {
                let value = th!(reader.read_u8(), Error::Io);
                Value::Boolean(value == 1)
            }
            DataType::Integer8 => {
                let value = th!(reader.read_i8(), Error::Io);
                Value::Numeric(value.to_string())
            }
            DataType::Integer16 => {
                let value = th!(reader.read_i16::<byteorder::LittleEndian>(), Error::Io);
                Value::Numeric(value.to_string())
            }
            DataType::Integer32 => {
                let value = th!(reader.read_i32::<byteorder::LittleEndian>(), Error::Io);
                Value::Numeric(value.to_string())
            }
            DataType::Integer64 => {
                let value = th!(reader.read_i64::<byteorder::LittleEndian>(), Error::Io);
                Value::Numeric(value.to_string())
            }
            DataType::Integer128 => {
                let value = th!(reader.read_i128::<byteorder::LittleEndian>(), Error::Io);
                Value::Numeric(value.to_string())
            }
            DataType::Float32 => {
                let value = th!(reader.read_f32::<byteorder::LittleEndian>(), Error::Io);
                Value::Numeric(value.to_string())
            }
            DataType::Float64 => {
                let value = th!(reader.read_f64::<byteorder::LittleEndian>(), Error::Io);
                Value::Numeric(value.to_string())
            }
            DataType::Null => Value::Null,
            _ => return Err(Error::ReadInvalidDataType),
        };

        values.push(value);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_write_and_reader_header() {
        let header_path = "header.bin";
        let original_properties = vec![
            Property {
                data_type: DataType::Varchar(30),
                label: "name".to_string(),
            },
            Property {
                data_type: DataType::Integer32,
                label: "age".to_string(),
            },
            Property {
                data_type: DataType::Boolean,
                label: "active".to_string(),
            },
        ];

        write_header(header_path, &original_properties).unwrap();

        let properties = read_header(header_path).unwrap();
        assert_eq!(properties, original_properties);

        fs::remove_file("header.bin").unwrap();
    }

    #[test]
    fn test_write_and_read_values() {
        let header = vec![
            Property {
                data_type: DataType::Varchar(30),
                label: "name".to_string(),
            },
            Property {
                data_type: DataType::Integer32,
                label: "age".to_string(),
            },
            Property {
                data_type: DataType::Boolean,
                label: "active".to_string(),
            },
        ];

        let values = vec![
            Value::String("John Doe".to_string()),
            Value::Numeric("30".to_string()),
            Value::Boolean(true),
        ];

        write_values("values.bin", &header, &values).unwrap();

        let read_values = read_values("values.bin", &header).unwrap();
        assert_eq!(values, read_values);

        fs::remove_file("values.bin").unwrap();
    }
}
