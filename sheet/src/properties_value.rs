use byteorder::ReadBytesExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use valu3::prelude::*;

use crate::header::{DataType, Header};
use crate::{th, th_none, Error, FALSE_BIN_VALUE, NULL_BIN_VALUE, TRUE_BIN_VALUE};

/// Represents the data types that can be stored in a property.
/// The data types are used to serialize and deserialize the data.
pub enum Data {
    Null,
    Boolean(bool),
    String(String),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    I128(i128),
    F32(f32),
    F64(f64),
}

macro_rules! write_data {
    ($file:expr, $data:expr, $prop:expr, $data_type:ident) => {
        match $prop.data_type {
            DataType::$data_type => {
                th!($file.write_all(&$data.to_le_bytes()), Error::Io);
            }
            _ => return Err(Error::WriteInvalidDataType),
        }
    };
}

macro_rules! read_data {
    ($reader:expr, $prop:expr, $data_type:ident, $method:ident) => {
        match $prop.data_type {
            DataType::$data_type => {
                let value = th!($reader.$method::<byteorder::LittleEndian>(), Error::Io);
                Data::$data_type(value)
            }
            _ => return Err(Error::ReadInvalidDataType),
        }
    };
}

/// Writes the properties data to a file.
/// The data is serialized using the header and the values.
/// The header is used to determine the data type of each property.
/// The values are used to write the data to the file.
/// The data is written in binary format.
/// The data is written in the same order as the header.
pub fn write_properties_data(path: &str, header: &Header, values: &Vec<Data>) -> Result<(), Error> {
    let mut file = BufWriter::new(th!(File::create(path), Error::Io));

    for (index, value) in values.iter().enumerate() {
        match value {
            Data::Boolean(data) => {
                th!(
                    file.write_all(&[if *data {
                        TRUE_BIN_VALUE
                    } else {
                        FALSE_BIN_VALUE
                    }]),
                    Error::Io
                );
            }
            Data::String(data) => {
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
            Data::U8(data) => {
                write_data!(file, data, header[index], U8);
            }
            Data::U16(data) => {
                write_data!(file, data, header[index], U16);
            }
            Data::U32(data) => {
                write_data!(file, data, header[index], U32);
            }
            Data::U64(data) => {
                write_data!(file, data, header[index], U64);
            }
            Data::U128(data) => {
                write_data!(file, data, header[index], U128);
            }
            Data::I8(data) => {
                write_data!(file, data, header[index], I8);
            }
            Data::I16(data) => {
                write_data!(file, data, header[index], I16);
            }
            Data::I32(data) => {
                write_data!(file, data, header[index], I32);
            }
            Data::I64(data) => {
                write_data!(file, data, header[index], I64);
            }
            Data::I128(data) => {
                write_data!(file, data, header[index], I128);
            }
            Data::F32(data) => {
                write_data!(file, data, header[index], F32);
            }
            Data::F64(data) => {
                write_data!(file, data, header[index], F64);
            }
            Data::Null => {
                th!(file.write_all(&[NULL_BIN_VALUE]), Error::Io);
            }
        }
    }

    th!(file.flush(), Error::Io);
    Ok(())
}

/// Reads the properties data from a file.
/// The data is deserialized using the header.
/// The header is used to determine the data type of each property.
/// The data is read in binary format.
/// The data is read in the same order as the header.
/// The data is returned as a vector of Data.
/// The data is returned in the same order as the header.
/// # Example
/// ```
/// use sheet::properties_value::{read_properties_data, write_properties_data};
/// use sheet::header::{DataType, Property};
/// use sheet::Error;
///
/// fn main() -> Result<(), Error> {
///     let header = vec![
///         Property {
///             data_type: DataType::Varchar(30),
///             label: "varchar".to_string(),
///         },
///         Property {
///             data_type: DataType::Boolean,
///             label: "boolean".to_string(),
///         },
///         Property {
///             data_type: DataType::Text,
///             label: "text".to_string(),
///         },
///     };
/// 
///    let values = vec![
///       Data::String("varchar".to_string()),
///       Data::Boolean(true),
///       Data::String("text".to_string()),
///    ];
/// 
///    write_properties_data("values.bin", &header, &values)?;
pub fn read_properties_data(path: &str, header: &Header) -> Result<Vec<Data>, Error> {
    let file = th!(File::open(path), Error::Io);
    let mut reader = BufReader::new(file);
    let mut values = Vec::new();

    for prop in header {
        let value = match prop.data_type {
            DataType::Varchar(size) => {
                let mut buffer = vec![0u8; size as usize];

                th!(reader.read_exact(&mut buffer), Error::Io);

                Data::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Text => {
                let size = th!(reader.read_u32::<byteorder::LittleEndian>(), Error::Io) as usize;

                let mut buffer = vec![0; size];

                th!(reader.read_exact(&mut buffer), Error::Io);

                Data::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Boolean => {
                let value = th!(reader.read_u8(), Error::Io);
                Data::Boolean(value == 1)
            }
            DataType::I8 => {
                let value = th!(reader.read_i8(), Error::Io);
                Data::I8(value)
            }
            DataType::I16 => read_data!(reader, prop, I16, read_i16),
            DataType::I32 => read_data!(reader, prop, I32, read_i32),
            DataType::I64 => read_data!(reader, prop, I64, read_i64),
            DataType::I128 => read_data!(reader, prop, I128, read_i128),
            DataType::U8 => {
                let value = th!(reader.read_u8(), Error::Io);
                Data::U8(value)
            }
            DataType::U16 => read_data!(reader, prop, U16, read_u16),
            DataType::U32 => read_data!(reader, prop, U32, read_u32),
            DataType::U64 => read_data!(reader, prop, U64, read_u64),
            DataType::U128 => read_data!(reader, prop, U128, read_u128),
            DataType::F32 => read_data!(reader, prop, F32, read_f32),
            DataType::F64 => read_data!(reader, prop, F64, read_f64),
            DataType::Null => Data::Null,
            _ => return Err(Error::ReadInvalidDataType),
        };

        values.push(value);
    }

    Ok(values)
}

pub fn write_properties_value(
    path: &str,
    header: &Header,
    values: &Vec<Value>,
) -> Result<(), Error> {
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

pub fn read_properties_value(path: &str, header: &Header) -> Result<Vec<Value>, Error> {
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

    use crate::header::Property;

    use super::*;

    #[test]
    fn test_write_and_read_properties() {
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
            Value::from("text".to_string().repeat(100)),
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

        write_properties_value("values.bin", &header, &values).unwrap();

        let read_properties = read_properties_value("values.bin", &header).unwrap();
        assert_eq!(values, read_properties);

        fs::remove_file("values.bin").unwrap();
    }
}
