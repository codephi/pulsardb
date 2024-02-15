use byteorder::ReadBytesExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use crate::header::{DataType, Header};
use crate::{th, Error, FALSE_BIN_VALUE, NULL_BIN_VALUE, TRUE_BIN_VALUE};

#[derive(Debug, PartialEq)]
pub enum DataValue {
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
    ($buffer_writer:expr, $data:expr, $prop:expr, $data_type:ident) => {
        match $prop.data_type {
            DataType::$data_type => {
                th!($buffer_writer.write_all(&$data.to_le_bytes()), Error::Io);
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
                DataValue::$data_type(value)
            }
            _ => return Err(Error::ReadInvalidDataType),
        }
    };
}

/// Write properties to a file
/// # Example
/// ```
/// let buffer_writer = &mut BufWriter::new(File::create("values.bin").unwrap());
/// let header = vec![
///    PropertyHeader::from(("varchar", DataType::Varchar(30))),
///    PropertyHeader::from(("boolean", DataType::Boolean)),
///    PropertyHeader::from(("text", DataType::Text)),
///    PropertyHeader::from(("i8", DataType::I8)),
///    PropertyHeader::from(("i16", DataType::I16)),
///    PropertyHeader::from(("i32", DataType::I32)),
///    PropertyHeader::from(("i64", DataType::I64)),
///    PropertyHeader::from(("i128", DataType::I128)),
///    PropertyHeader::from(("u8", DataType::U8)),
///    PropertyHeader::from(("u16", DataType::U16)),
///    PropertyHeader::from(("u32", DataType::U32)),
///    PropertyHeader::from(("u64", DataType::U64)),
///    PropertyHeader::from(("f32", DataType::F32)),
///    PropertyHeader::from(("f64", DataType::F64)),
/// ];
/// let values = vec![
///    DataValue::String(format!("{: <30}", "varchar")),
///    DataValue::Boolean(true),
///    DataValue::String("text".to_string()),
///    DataValue::I8(8),
///    DataValue::I16(16),
///    DataValue::I32(32),
///    DataValue::I64(64),
///    DataValue::I128(128),
///    DataValue::U8(8),
///    DataValue::U16(16),
///    DataValue::U32(32),
///    DataValue::U64(64),
///    DataValue::F32(32.0),
///    DataValue::F64(64.0),
/// ];
///
/// write_properties(buffer_writer, &header, &values).unwrap();
/// buffer_writer.flush().unwrap();
/// ```
pub fn write_properties(
    buffer_writer: &mut BufWriter<File>,
    header: &Header,
    values: &Vec<DataValue>,
) -> Result<(), Error> {
    for (index, value) in values.iter().enumerate() {
        match value {
            DataValue::Boolean(data) => {
                th!(
                    buffer_writer.write_all(&[if *data {
                        TRUE_BIN_VALUE
                    } else {
                        FALSE_BIN_VALUE
                    }]),
                    Error::Io
                );
            }
            DataValue::String(data) => {
                let prop = &header[index];
                match prop.data_type {
                    DataType::Varchar(size) => {
                        if data.len() > size as usize {
                            return Err(Error::VarcharSize);
                        }

                        let mut buffer = vec![0u8; size as usize];
                        buffer[..data.len()].copy_from_slice(data.as_bytes());

                        th!(buffer_writer.write_all(&buffer), Error::Io);
                    }
                    DataType::Text => {
                        th!(
                            buffer_writer.write_all(&(data.len() as u32).to_le_bytes()),
                            Error::Io
                        );
                        th!(buffer_writer.write_all(data.as_bytes()), Error::Io);
                    }
                    _ => return Err(Error::WriteInvalidDataType),
                }
            }
            DataValue::U8(data) => {
                write_data!(buffer_writer, data, header[index], U8);
            }
            DataValue::U16(data) => {
                write_data!(buffer_writer, data, header[index], U16);
            }
            DataValue::U32(data) => {
                write_data!(buffer_writer, data, header[index], U32);
            }
            DataValue::U64(data) => {
                write_data!(buffer_writer, data, header[index], U64);
            }
            DataValue::U128(data) => {
                write_data!(buffer_writer, data, header[index], U128);
            }
            DataValue::I8(data) => {
                write_data!(buffer_writer, data, header[index], I8);
            }
            DataValue::I16(data) => {
                write_data!(buffer_writer, data, header[index], I16);
            }
            DataValue::I32(data) => {
                write_data!(buffer_writer, data, header[index], I32);
            }
            DataValue::I64(data) => {
                write_data!(buffer_writer, data, header[index], I64);
            }
            DataValue::I128(data) => {
                write_data!(buffer_writer, data, header[index], I128);
            }
            DataValue::F32(data) => {
                write_data!(buffer_writer, data, header[index], F32);
            }
            DataValue::F64(data) => {
                write_data!(buffer_writer, data, header[index], F64);
            }
            DataValue::Null => {
                th!(buffer_writer.write_all(&[NULL_BIN_VALUE]), Error::Io);
            }
        }
    }

    Ok(())
}

/// Read properties from a file
/// # Example
/// ```
/// let buffer_reader = &mut BufReader::new(File::open("values.bin").unwrap());
/// let header = vec![
///    PropertyHeader::from(("varchar", DataType::Varchar(30))),
///    PropertyHeader::from(("boolean", DataType::Boolean)),
///    PropertyHeader::from(("text", DataType::Text)),
///    PropertyHeader::from(("i8", DataType::I8)),
///    PropertyHeader::from(("i16", DataType::I16)),
///    PropertyHeader::from(("i32", DataType::I32)),
///    PropertyHeader::from(("i64", DataType::I64)),
///    PropertyHeader::from(("i128", DataType::I128)),
///    PropertyHeader::from(("u8", DataType::U8)),
///    PropertyHeader::from(("u16", DataType::U16)),
///    PropertyHeader::from(("u32", DataType::U32)),
///    PropertyHeader::from(("u64", DataType::U64)),
///    PropertyHeader::from(("f32", DataType::F32)),
///    PropertyHeader::from(("f64", DataType::F64)),
/// ];
/// let values = vec![
///    DataValue::String(format!("{: <30}", "varchar")),
///    DataValue::Boolean(true),
///    DataValue::String("text".to_string()),
///    DataValue::I8(8),
///    DataValue::I16(16),
///    DataValue::I32(32),
///    DataValue::I64(64),
///    DataValue::I128(128),
///    DataValue::U8(8),
///    DataValue::U16(16),
///    DataValue::U32(32),
///    DataValue::U64(64),
///    DataValue::F32(32.0),
///    DataValue::F64(64.0),
/// ];
///
/// let read_properties = read_properties(buffer_reader, &header).unwrap();
/// assert_eq!(values, read_properties);
/// ```
pub fn read_properties(
    buffer_reader: &mut BufReader<File>,
    header: &Header,
) -> Result<Vec<DataValue>, Error> {
    let mut values = Vec::new();

    for prop in header {
        let value = match prop.data_type {
            DataType::Varchar(size) => {
                let mut buffer = vec![0u8; size as usize];

                th!(buffer_reader.read_exact(&mut buffer), Error::Io);

                DataValue::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Text => {
                let size = th!(
                    buffer_reader.read_u32::<byteorder::LittleEndian>(),
                    Error::Io
                ) as usize;

                let mut buffer = vec![0; size];

                th!(buffer_reader.read_exact(&mut buffer), Error::Io);

                DataValue::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Boolean => {
                let value = th!(buffer_reader.read_u8(), Error::Io);
                DataValue::Boolean(value == 1)
            }
            DataType::I8 => {
                let value = th!(buffer_reader.read_i8(), Error::Io);
                DataValue::I8(value)
            }
            DataType::I16 => read_data!(buffer_reader, prop, I16, read_i16),
            DataType::I32 => read_data!(buffer_reader, prop, I32, read_i32),
            DataType::I64 => read_data!(buffer_reader, prop, I64, read_i64),
            DataType::I128 => read_data!(buffer_reader, prop, I128, read_i128),
            DataType::U8 => {
                let value = th!(buffer_reader.read_u8(), Error::Io);
                DataValue::U8(value)
            }
            DataType::U16 => read_data!(buffer_reader, prop, U16, read_u16),
            DataType::U32 => read_data!(buffer_reader, prop, U32, read_u32),
            DataType::U64 => read_data!(buffer_reader, prop, U64, read_u64),
            DataType::U128 => read_data!(buffer_reader, prop, U128, read_u128),
            DataType::F32 => read_data!(buffer_reader, prop, F32, read_f32),
            DataType::F64 => read_data!(buffer_reader, prop, F64, read_f64),
            DataType::Null => DataValue::Null,
            _ => return Err(Error::ReadInvalidDataType),
        };

        values.push(value);
    }

    Ok(values)
}

// pub fn read_properties_by_position(
//     buffer_reader: &mut BufReader<File>,
//     positions: &Vec<u64>,
// ) -> Result<Vec<DataValue>, Error> {
// }

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::header::PropertyHeader;

    use super::*;

    #[test]
    fn test_write_and_read_properties() {
        let header = vec![
            PropertyHeader::from(("varchar", DataType::Varchar(30))),
            PropertyHeader::from(("boolean", DataType::Boolean)),
            PropertyHeader::from(("text", DataType::Text)),
            PropertyHeader::from(("i8", DataType::I8)),
            PropertyHeader::from(("i16", DataType::I16)),
            PropertyHeader::from(("i32", DataType::I32)),
            PropertyHeader::from(("i64", DataType::I64)),
            PropertyHeader::from(("i128", DataType::I128)),
            PropertyHeader::from(("u8", DataType::U8)),
            PropertyHeader::from(("u16", DataType::U16)),
            PropertyHeader::from(("u32", DataType::U32)),
            PropertyHeader::from(("u64", DataType::U64)),
            PropertyHeader::from(("f32", DataType::F32)),
            PropertyHeader::from(("f64", DataType::F64)),
        ];

        let values = vec![
            DataValue::String(format!("{: <30}", "varchar")),
            DataValue::Boolean(true),
            DataValue::String("text".to_string()),
            DataValue::I8(8),
            DataValue::I16(16),
            DataValue::I32(32),
            DataValue::I64(64),
            DataValue::I128(128),
            DataValue::U8(8),
            DataValue::U16(16),
            DataValue::U32(32),
            DataValue::U64(64),
            DataValue::F32(32.0),
            DataValue::F64(64.0),
        ];

        let path = "values.bin";

        let buffer_writer = &mut BufWriter::new(File::create(path).unwrap());

        write_properties(buffer_writer, &header, &values).unwrap();

        buffer_writer.flush().unwrap();

        let buffer_reader = &mut BufReader::new(File::open(path).unwrap());

        let read_properties = read_properties(buffer_reader, &header).unwrap();
        assert_eq!(values, read_properties);

        fs::remove_file("values.bin").unwrap();
    }
}
