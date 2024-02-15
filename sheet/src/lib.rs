use byteorder::ReadBytesExt;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use valu3::prelude::*;

const DATA_TYPE_UNDEFINED: u8 = 0;
const DATA_TYPE_NULL: u8 = 1;
const DATA_TYPE_BOOLEAN: u8 = 2;
const DATA_TYPE_VARCHAR: u8 = 3;
const DATA_TYPE_TEXT: u8 = 4;

const DATA_TYPE_U8: u8 = 5;
const DATA_TYPE_U16: u8 = 6;
const DATA_TYPE_U32: u8 = 7;
const DATA_TYPE_U64: u8 = 8;
const DATA_TYPE_U128: u8 = 9;

const DATA_TYPE_I8: u8 = 10;
const DATA_TYPE_I16: u8 = 11;
const DATA_TYPE_I32: u8 = 12;
const DATA_TYPE_I64: u8 = 13;
const DATA_TYPE_I128: u8 = 14;

const DATA_TYPE_F32: u8 = 15;
const DATA_TYPE_F64: u8 = 16;

const NULL_BIN_VALUE: u8 = 0;
const FALSE_BIN_VALUE: u8 = 0;
const TRUE_BIN_VALUE: u8 = 1;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    WriteInvalidDataType,
    ReadInvalidDataType,
    VarcharSize,
    NumberParse,
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

macro_rules! th_none {
    ($result:expr, $error:expr) => {
        match $result {
            Some(value) => value,
            None => return Err($error),
        }
    };
}

#[derive(Debug, PartialEq, Clone)]
enum DataType {
    Null,
    Boolean,
    Text,
    Varchar(u32),
    I8,
    I16,
    I32,
    I64,
    I128,
    U8,
    U16,
    U32,
    U64,
    U128,
    F32,
    F64,
    Undefined,
}

impl Into<u8> for DataType {
    fn into(self) -> u8 {
        match self {
            DataType::Null => DATA_TYPE_NULL,
            DataType::Boolean => DATA_TYPE_BOOLEAN,
            DataType::Text => DATA_TYPE_TEXT,
            DataType::Varchar(_) => DATA_TYPE_VARCHAR,
            DataType::Undefined => DATA_TYPE_UNDEFINED,
            DataType::I8 => DATA_TYPE_I8,
            DataType::I16 => DATA_TYPE_I16,
            DataType::I32 => DATA_TYPE_I32,
            DataType::I64 => DATA_TYPE_I64,
            DataType::I128 => DATA_TYPE_I128,
            DataType::U8 => DATA_TYPE_U8,
            DataType::U16 => DATA_TYPE_U16,
            DataType::U32 => DATA_TYPE_U32,
            DataType::U64 => DATA_TYPE_U64,
            DataType::U128 => DATA_TYPE_U128,
            DataType::F32 => DATA_TYPE_F32,
            DataType::F64 => DATA_TYPE_F64,
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
            DATA_TYPE_I8 => DataType::I8,
            DATA_TYPE_I16 => DataType::I16,
            DATA_TYPE_I32 => DataType::I32,
            DATA_TYPE_I64 => DataType::I64,
            DATA_TYPE_I128 => DataType::I128,
            DATA_TYPE_U8 => DataType::U8,
            DATA_TYPE_U16 => DataType::U16,
            DATA_TYPE_U32 => DataType::U32,
            DATA_TYPE_U64 => DataType::U64,
            DATA_TYPE_U128 => DataType::U128,
            DATA_TYPE_F32 => DataType::F32,
            DATA_TYPE_F64 => DataType::F64,
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
            DataType::I8 => 1,
            DataType::I16 => 2,
            DataType::I32 => 4,
            DataType::I64 => 8,
            DataType::I128 => 16,
            DataType::U8 => 1,
            DataType::U16 => 2,
            DataType::U32 => 4,
            DataType::U64 => 8,
            DataType::U128 => 16,
            DataType::F32 => 4,
            DataType::F64 => 8,
            DataType::Undefined => 0,
            _ => 0,
        }
    }
}

pub type Header = Vec<Property>;

pub fn write_header(path: &str, properties: &Header) -> Result<(), Error> {
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

pub fn read_header(path: &str) -> Result<Header, Error> {
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

pub fn write_values(path: &str, header: &Header, values: &Vec<Value>) -> Result<(), Error> {
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
            Value::Number(data) => {
                let prop = &header[index];
                match prop.data_type {
                    DataType::I8 => {
                        let value = th_none!(data.get_i8(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::I16 => {
                        let value = th_none!(data.get_i16(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::I32 => {
                        let value = th_none!(data.get_i32(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::I64 => {
                        let value = th_none!(data.get_i64(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::I128 => {
                        let value = th_none!(data.get_i128(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::U8 => {
                        let value = th_none!(data.get_u8(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::U16 => {
                        let value = th_none!(data.get_u16(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::U32 => {
                        let value = th_none!(data.get_u32(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::U64 => {
                        let value = th_none!(data.get_u64(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::U128 => {
                        let value = th_none!(data.get_u128(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::F32 => {
                        let value = th_none!(data.get_f32(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    DataType::F64 => {
                        let value = th_none!(data.get_f64(), Error::NumberParse);
                        th!(file.write_all(&value.to_le_bytes()), Error::Io);
                    }
                    _ => return Err(Error::WriteInvalidDataType),
                }
            }
            Value::Null => {
                th!(file.write_all(&[NULL_BIN_VALUE]), Error::Io);
            }
            Value::Array(_) | Value::Object(_) => {
                let value_string = value.to_json(JsonMode::Inline);
                let value_bytes = value_string.as_bytes();
                th!(
                    file.write_all(&(value_bytes.len() as u32).to_le_bytes()),
                    Error::Io
                );
            }
            Value::Undefined => return Err(Error::WriteInvalidDataType),
            Value::DateTime(date) => {
                let value = match date.timestamp() {
                    Some(timestamp) => timestamp,
                    None => return Err(Error::WriteInvalidDataType),
                };

                th!(file.write_all(&value.to_le_bytes()), Error::Io);
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

                Value::from(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Text => {
                let size = th!(reader.read_u32::<byteorder::LittleEndian>(), Error::Io) as usize;

                let mut buffer = vec![0; size];

                th!(reader.read_exact(&mut buffer), Error::Io);

                Value::from(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Boolean => {
                let value = th!(reader.read_u8(), Error::Io);
                Value::Boolean(value == 1)
            }
            DataType::I8 => {
                let value = th!(reader.read_i8(), Error::Io);
                Value::from(value)
            }
            DataType::I16 => {
                let value = th!(reader.read_i16::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::I32 => {
                let value = th!(reader.read_i32::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::I64 => {
                let value = th!(reader.read_i64::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::I128 => {
                let value = th!(reader.read_i128::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::U8 => {
                let value = th!(reader.read_u8(), Error::Io);
                Value::from(value)
            }
            DataType::U16 => {
                let value = th!(reader.read_u16::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::U32 => {
                let value = th!(reader.read_u32::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::U64 => {
                let value = th!(reader.read_u64::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::U128 => {
                let value = th!(reader.read_u128::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::F32 => {
                let value = th!(reader.read_f32::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
            }
            DataType::F64 => {
                let value = th!(reader.read_f64::<byteorder::LittleEndian>(), Error::Io);
                Value::from(value)
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
                data_type: DataType::I32,
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
                label: "varchar".to_string(),
            },
            Property {
                data_type: DataType::Boolean,
                label: "boolean".to_string(),
            },
            Property {
                data_type: DataType::Text,
                label: "text".to_string(),
            },
            Property {
                data_type: DataType::I8,
                label: "i8".to_string(),
            },
            Property {
                data_type: DataType::I16,
                label: "i16".to_string(),
            },
            Property {
                data_type: DataType::I32,
                label: "i32".to_string(),
            },
            Property {
                data_type: DataType::I64,
                label: "i64".to_string(),
            },
            Property {
                data_type: DataType::I128,
                label: "i128".to_string(),
            },
            Property {
                data_type: DataType::U8,
                label: "u8".to_string(),
            },
            Property {
                data_type: DataType::U16,
                label: "u16".to_string(),
            },
            Property {
                data_type: DataType::U32,
                label: "u32".to_string(),
            },
            Property {
                data_type: DataType::U64,
                label: "u64".to_string(),
            },
            Property {
                data_type: DataType::F32,
                label: "f32".to_string(),
            },
            Property {
                data_type: DataType::F64,
                label: "f64".to_string(),
            },
        ];

        let values = vec![
            Value::from(format!("{: <30}", "varchar")),
            Value::from(true),
            Value::from("text".to_string()),
            Value::from(-1 as i8),
            Value::from(-2 as i16),
            Value::from(-3 as i32),
            Value::from(-4 as i64),
            Value::from(-5 as i128),
            Value::from(1 as u8),
            Value::from(2 as u16),
            Value::from(3 as u32),
            Value::from(4 as u64),
            Value::from(1.0 as f32),
            Value::from(2.0 as f64),
        ];

        write_values("values.bin", &header, &values).unwrap();

        let read_values = read_values("values.bin", &header).unwrap();
        assert_eq!(values, read_values);

        fs::remove_file("values.bin").unwrap();
    }
}
