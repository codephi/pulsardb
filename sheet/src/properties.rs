use byteorder::ReadBytesExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};

use crate::header::{DataType, Header, PropertyHeader};
use crate::{
    th, th_msg, th_none, Error, DEFAULT_SIZE_U32, FALSE_BIN_VALUE, NULL_BIN_VALUE, TRUE_BIN_VALUE,
};

#[derive(Debug, PartialEq, Clone)]
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

/// Builder for Data struct
/// # Example
/// ```
/// let header = Header::from(vec![
///   ("varchar", DataType::Varchar(30)),
///   ("boolean", DataType::Boolean),
///   ("text", DataType::Text),
///   ("i8", DataType::I8),
/// ]);
///
/// let values = vec![
///   DataValue::String(format!("{: <30}", "varchar")),
///   DataValue::Boolean(true),
///   DataValue::String("text".to_string()),
///   DataValue::I8(8),
/// ];
///
/// let builder = BuilderData::from_properties(&header, "values.bin", values);
/// let data = builder.build();
/// ```
pub struct BuilderData<'a> {
    header: &'a Header,
    values: Vec<DataValue>,
    dynamic_values: Vec<DataValue>,
}

impl<'a> BuilderData<'a> {
    /// Create a new BuilderData
    pub fn new(header: &'a Header) -> Self {
        Self {
            header,
            values: Vec::with_capacity(header.len()),
            dynamic_values: Vec::new(),
        }
    }

    /// Create a new BuilderData from properties without orderer values.
    /// This method is unsafe because it does not check the order of the values.
    pub fn from_properties_unsafe(header: &'a Header, values: Vec<DataValue>) -> Self {
        let builder = {
            let mut builder = Self::new(header);

            builder.values = values;

            builder
        };

        builder
    }

    /// Create a new BuilderData from properties, ordering the values by the header
    pub fn from_properties(header: &'a Header, values: Vec<DataValue>) -> Self {
        let builder = {
            let mut builder = Self::new(header);

            for value in values {
                builder.add_property(value);
            }

            builder
        };

        builder
    }

    /// Add a property to the builder
    pub fn add_property(&mut self, value: DataValue) {
        let position = self.values.len() + self.dynamic_values.len();
        let prop = self.header.get_by_original_position(position).unwrap();

        if prop.is_dynamic_size() {
            self.dynamic_values.push(value);
        } else {
            self.values.push(value);
        }
    }

    /// Build the Data struct
    pub fn build(self) -> Data<'a> {
        let mut values = self.values.clone();
        values.append(&mut self.dynamic_values.clone());

        Data {
            header: self.header,
            values,
        }
    }
}

/// Data struct
/// # Example
/// ```
/// use std::fs;
/// use sheet::{DataType, DataValue, Header, BuilderData};
///
/// let header = Header::from(vec![
///   ("varchar", DataType::Varchar(30)),
///   ("boolean", DataType::Boolean),
///   ("text", DataType::Text),
///   ("i8", DataType::I8),
/// ]);
///
/// let values = vec![
///   DataValue::String(format!("{: <30}", "varchar")),
///   DataValue::Boolean(true),
///   DataValue::String("text".to_string()),
///   DataValue::I8(8),
/// ];
///
/// let mut data = BuilderData::from_properties(&header, values).build();
///
/// let path = "test_data_struct.bin";
///
/// assert!(data.write(path).is_ok());
///
/// assert!(data.read(path).is_ok());
///
/// assert_eq!(values, *data.get_values());
///
/// fs::remove(path).unwrap();
/// ```
#[derive(Debug)]
pub struct Data<'a> {
    header: &'a Header,
    values: Vec<DataValue>,
}

impl<'a> Data<'a> {
    /// Create a new Data struct
    pub fn new(header: &'a Header) -> Self {
        Self {
            header,
            values: Vec::with_capacity(header.len()),
        }
    }
    /// Write the Data struct to a file
    pub fn write(&mut self, path: &str) -> Result<(), Error> {
        let mut buffer_writer = BufWriter::new(th_msg!(File::create(path), Error::Io));

        if let Err(err) = write_properties(&mut buffer_writer, self.header, &self.values) {
            return Err(err);
        }

        th!(buffer_writer.flush(), Error::WriteProperties);

        Ok(())
    }

    /// Read the Data struct from a file
    pub fn read(&mut self, path: &str) -> Result<(), Error> {
        let mut buffer_reader = BufReader::new(th_msg!(File::open(path), Error::Io));
        self.values = read_properties(&mut buffer_reader, self.header)?;
        Ok(())
    }

    /// Read the Data struct from a file by original positions
    pub fn read_by_original_positions(
        &mut self,
        path: &str,
        property_headers_original_positions: &Vec<usize>,
    ) -> Result<(), Error> {
        let mut buffer_reader = BufReader::new(th_msg!(File::open(path), Error::Io));
        let property_headers = property_headers_original_positions
            .iter()
            .filter_map(|original_position| {
                self.header.get_by_original_position(*original_position)
            })
            .collect::<Vec<_>>();

        self.values =
            read_properties_by_byte_position(&mut buffer_reader, self.header, &property_headers)?;
        Ok(())
    }

    /// Read the Data struct from a file by positions
    pub fn read_by_positions(
        &mut self,
        path: &str,
        property_headers_positions: &Vec<usize>,
    ) -> Result<(), Error> {
        let mut buffer_reader = BufReader::new(th_msg!(File::open(path), Error::Io));
        let property_headers = property_headers_positions
            .iter()
            .filter_map(|position| self.header.get(*position))
            .collect::<Vec<_>>();

        self.values =
            read_properties_by_byte_position(&mut buffer_reader, self.header, &property_headers)?;
        Ok(())
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&DataValue> {
        self.values.get(index)
    }

    /// Get the values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Get the values
    pub fn get_by_label(&self, label: &[u8]) -> Result<&DataValue, Error> {
        let label = th!(self.header.get_by_label(label), Error::LabelNotFound);
        let label = th_none!(self.values.get(label.get_position()), Error::LabelNotFound);
        Ok(label)
    }

    /// Get the values
    pub fn get_values(&self) -> &Vec<DataValue> {
        &self.values
    }
}

macro_rules! write_data {
    ($buffer_writer:expr, $data:expr, $prop:expr, $data_type:ident) => {
        match $prop.get_data_type() {
            DataType::$data_type => {
                th_msg!($buffer_writer.write_all(&$data.to_le_bytes()), Error::Io);
            }
            _ => return Err(Error::WriteInvalidDataTypeNumber),
        }
    };
}

macro_rules! read_data {
    ($reader:expr, $prop:expr, $data_type:ident, $method:ident) => {
        match $prop.get_data_type() {
            DataType::$data_type => {
                let value = th_msg!($reader.$method::<byteorder::LittleEndian>(), Error::Io);
                DataValue::$data_type(value)
            }
            _ => return Err(Error::ReadInvalidDataType),
        }
    };
}

macro_rules! read_data_by_byte_position {
    ($reader:expr, $prop:expr, $data_type:ident, $method:ident) => {{
        let pos = $prop.get_byte_position().unwrap();

        th_msg!(
            $reader.seek(std::io::SeekFrom::Start(pos as u64)),
            Error::Io
        );

        match $prop.get_data_type() {
            DataType::$data_type => {
                let value = th_msg!($reader.$method::<byteorder::LittleEndian>(), Error::Io);
                DataValue::$data_type(value)
            }
            _ => return Err(Error::ReadInvalidDataType),
        }
    }};
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
/// Properties pattern:
/// | data_type (only Text) | value | data_type (only Text) | value | ...
pub fn write_properties(
    buffer_writer: &mut BufWriter<File>,
    header: &Header,
    values: &Vec<DataValue>,
) -> Result<(), Error> {
    for prop in header.headers_iter() {
        let value = th_none!(
            values.get(prop.get_position()),
            Error::WritePropertiesGetHeader
        );

        match value {
            DataValue::Boolean(data) => {
                th_msg!(
                    buffer_writer.write_all(&[if *data {
                        TRUE_BIN_VALUE
                    } else {
                        FALSE_BIN_VALUE
                    }]),
                    Error::Io
                );
            }
            DataValue::String(data) => match prop.get_data_type() {
                DataType::Varchar(size) => {
                    let size = *size;
                    if data.len() > size as usize {
                        return Err(Error::VarcharSize);
                    }

                    let mut buffer = vec![0u8; size as usize];
                    buffer[..data.len()].copy_from_slice(data.as_bytes());

                    th_msg!(buffer_writer.write_all(&buffer), Error::Io);
                }
                DataType::Text => {
                    th_msg!(
                        buffer_writer.write_all(&(data.len() as u32).to_le_bytes()),
                        Error::Io
                    );
                    th_msg!(buffer_writer.write_all(data.as_bytes()), Error::Io);
                }
                _ => {
                    return Err(Error::WriteInvalidDataTypeString(format!(
                        "DataValue is String, but Header is {}",
                        prop.get_data_type()
                    )))
                }
            },
            DataValue::U8(data) => {
                write_data!(buffer_writer, data, prop, U8);
            }
            DataValue::U16(data) => {
                write_data!(buffer_writer, data, prop, U16);
            }
            DataValue::U32(data) => {
                write_data!(buffer_writer, data, prop, U32);
            }
            DataValue::U64(data) => {
                write_data!(buffer_writer, data, prop, U64);
            }
            DataValue::U128(data) => {
                write_data!(buffer_writer, data, prop, U128);
            }
            DataValue::I8(data) => {
                write_data!(buffer_writer, data, prop, I8);
            }
            DataValue::I16(data) => {
                write_data!(buffer_writer, data, prop, I16);
            }
            DataValue::I32(data) => {
                write_data!(buffer_writer, data, prop, I32);
            }
            DataValue::I64(data) => {
                write_data!(buffer_writer, data, prop, I64);
            }
            DataValue::I128(data) => {
                write_data!(buffer_writer, data, prop, I128);
            }
            DataValue::F32(data) => {
                write_data!(buffer_writer, data, prop, F32);
            }
            DataValue::F64(data) => {
                write_data!(buffer_writer, data, prop, F64);
            }
            DataValue::Null => {
                th_msg!(buffer_writer.write_all(&[NULL_BIN_VALUE]), Error::Io);
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
///    ("varchar", DataType::Varchar(30)),
///    ("boolean", DataType::Boolean),
///    ("text", DataType::Text),
///    ("i8", DataType::I8),
///    ("i16", DataType::I16),
///    ("i32", DataType::I32),
///    ("i64", DataType::I64),
///    ("i128", DataType::I128),
///    ("u8", DataType::U8),
///    ("u16", DataType::U16),    
///    ("u32", DataType::U32),
///    ("u64", DataType::U64),
///    ("f32", DataType::F32),
///    ("f64", DataType::F64),
/// ];
///
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

    for prop in header.headers_iter() {
        let value = match prop.get_data_type() {
            DataType::Varchar(size) => {
                let size = *size;
                let mut buffer = vec![0u8; size as usize];

                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                DataValue::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Text => {
                let size = th_msg!(
                    buffer_reader.read_u32::<byteorder::LittleEndian>(),
                    Error::Io
                ) as usize;

                let mut buffer = vec![0u8; size];

                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                DataValue::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Boolean => {
                let value = th_msg!(buffer_reader.read_u8(), Error::Io);
                DataValue::Boolean(value == 1)
            }
            DataType::I8 => {
                let value = th_msg!(buffer_reader.read_i8(), Error::Io);
                DataValue::I8(value)
            }
            DataType::I16 => read_data!(buffer_reader, prop, I16, read_i16),
            DataType::I32 => read_data!(buffer_reader, prop, I32, read_i32),
            DataType::I64 => read_data!(buffer_reader, prop, I64, read_i64),
            DataType::I128 => read_data!(buffer_reader, prop, I128, read_i128),
            DataType::U8 => {
                let value = th_msg!(buffer_reader.read_u8(), Error::Io);
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

/// Read properties from a file by byte position
/// Different from read_properties, this function reads the properties by byte position
pub fn read_properties_by_byte_position(
    buffer_reader: &mut BufReader<File>,
    header: &Header,
    property_headers: &Vec<&PropertyHeader>,
) -> Result<Vec<DataValue>, Error> {
    let mut values = Vec::new();

    for prop in property_headers.into_iter() {
        let value = match prop.get_data_type() {
            DataType::Varchar(size) => {
                let pos = prop.get_byte_position().unwrap();
                th_msg!(
                    buffer_reader.seek(std::io::SeekFrom::Start(pos as u64)),
                    Error::Io
                );

                let size = *size;
                let mut buffer = vec![0u8; size as usize];

                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                DataValue::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Text => {
                let mut last_pos = header.get_last_byte_position_no_dynamic().unwrap() as u64;
                let dynamic_sizes_positions = header.get_dynamic_size_positions();
                let mut dynamic_iter = dynamic_sizes_positions.iter();

                loop {
                    match dynamic_iter.next() {
                        Some(position) => {
                            th_msg!(
                                buffer_reader.seek(std::io::SeekFrom::Start(last_pos)),
                                Error::Io
                            );
                            let size = th_msg!(
                                buffer_reader.read_u32::<byteorder::LittleEndian>(),
                                Error::Io
                            ) as usize;

                            if position == &prop.get_position() {
                                let mut buffer = vec![0u8; size];

                                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                                break DataValue::String(
                                    String::from_utf8(buffer).expect("UTF-8 decoding error"),
                                );
                            } else {
                                last_pos += (size + DEFAULT_SIZE_U32) as u64 + 1;
                            }
                        }
                        None => return Err(Error::NoGetBytePosition),
                    }
                }
            }
            DataType::Boolean => {
                let pos = prop.get_byte_position().unwrap();
                th_msg!(
                    buffer_reader.seek(std::io::SeekFrom::Start(pos as u64)),
                    Error::Io
                );

                let value = th_msg!(buffer_reader.read_u8(), Error::Io);
                DataValue::Boolean(value == 1)
            }
            DataType::I8 => {
                let pos = prop.get_byte_position().unwrap();
                th_msg!(
                    buffer_reader.seek(std::io::SeekFrom::Start(pos as u64)),
                    Error::Io
                );

                let value = th_msg!(buffer_reader.read_i8(), Error::Io);
                DataValue::I8(value)
            }
            DataType::I16 => {
                read_data_by_byte_position!(buffer_reader, prop, I16, read_i16)
            }
            DataType::I32 => {
                read_data_by_byte_position!(buffer_reader, prop, I32, read_i32)
            }
            DataType::I64 => {
                read_data_by_byte_position!(buffer_reader, prop, I64, read_i64)
            }
            DataType::I128 => {
                read_data_by_byte_position!(buffer_reader, prop, I128, read_i128)
            }
            DataType::U8 => {
                let pos = prop.get_byte_position().unwrap();
                th_msg!(
                    buffer_reader.seek(std::io::SeekFrom::Start(pos as u64)),
                    Error::Io
                );
                let value = th_msg!(buffer_reader.read_u8(), Error::Io);
                DataValue::U8(value)
            }
            DataType::U16 => {
                read_data_by_byte_position!(buffer_reader, prop, U16, read_u16)
            }
            DataType::U32 => {
                read_data_by_byte_position!(buffer_reader, prop, U32, read_u32)
            }
            DataType::U64 => {
                read_data_by_byte_position!(buffer_reader, prop, U64, read_u64)
            }
            DataType::U128 => {
                read_data_by_byte_position!(buffer_reader, prop, U128, read_u128)
            }
            DataType::F32 => {
                read_data_by_byte_position!(buffer_reader, prop, F32, read_f32)
            }
            DataType::F64 => {
                read_data_by_byte_position!(buffer_reader, prop, F64, read_f64)
            }
            DataType::Null => DataValue::Null,
            _ => return Err(Error::ReadInvalidDataType),
        };

        values.push(value);
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_write_and_read_properties() {
        let header = Header::from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
            ("i16", DataType::I16),
            ("i32", DataType::I32),
            ("i64", DataType::I64),
            ("i128", DataType::I128),
            ("u8", DataType::U8),
            ("u16", DataType::U16),
            ("u32", DataType::U32),
            ("u64", DataType::U64),
            ("f32", DataType::F32),
            ("f64", DataType::F64),
        ]);

        let values = vec![
            DataValue::String(format!("{: <30}", "varchar")),
            DataValue::Boolean(true),
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
            // Text is positioned at the end automatically because it is a dynamic size type
            DataValue::String("text".to_string()),
        ];

        let path: &str = "test_write_and_read_properties.bin";

        let buffer_writer = &mut BufWriter::new(File::create(path).unwrap());

        write_properties(buffer_writer, &header, &values).unwrap();

        buffer_writer.flush().unwrap();

        let buffer_reader = &mut BufReader::new(File::open(path).unwrap());

        let read_properties = read_properties(buffer_reader, &header).unwrap();
        assert_eq!(values, read_properties);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_data_struct() {
        let header = Header::from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ]);

        let values = vec![
            DataValue::String(format!("{: <30}", "varchar")),
            DataValue::Boolean(true),
            DataValue::String("text".to_string()),
            DataValue::I8(8),
        ];

        let values_new_order = vec![
            DataValue::String(format!("{: <30}", "varchar")),
            DataValue::Boolean(true),
            DataValue::I8(8),
            DataValue::String("text".to_string()),
        ];

        let mut data = BuilderData::from_properties(&header, values).build();

        let path = "test_data_struct.bin";

        assert!(data.write(path).is_ok());

        assert!(data.read(path).is_ok());

        assert_eq!(values_new_order, *data.get_values());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_data_struct_unsafe() {
        let header = Header::from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ]);

        let values = vec![
            DataValue::String(format!("{: <30}", "varchar")),
            DataValue::Boolean(true),
            DataValue::I8(8),
            DataValue::String("text".to_string()),
        ];
        let mut data = BuilderData::from_properties_unsafe(&header, values.clone()).build();

        let path = "test_data_struct_unsafe.bin";

        assert!(data.write(path).is_ok());

        assert!(data.read(path).is_ok());

        assert_eq!(values, *data.get_values());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_by_original_positions() {
        let header = Header::from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ]);

        let values = vec![
            DataValue::String(format!("{: <30}", "varchar")),
            DataValue::Boolean(true),
            DataValue::String("text".to_string()),
            DataValue::I8(8),
        ];

        let mut data = BuilderData::from_properties(&header, values.clone()).build();

        let path = "test_read_by_original_positions.bin";

        assert!(data.write(path).is_ok());

        let property_headers_original_positions = vec![1, 0, 3];

        assert!(data
            .read_by_original_positions(path, &property_headers_original_positions)
            .is_ok());

        let expected = property_headers_original_positions
            .iter()
            .map(|position| values.get(*position).unwrap().clone())
            .collect::<Vec<_>>();

        assert_eq!(expected, *data.get_values());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_by_positions(){
        let header = Header::from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ]);

        let values = vec![
            DataValue::String(format!("{: <30}", "varchar")),
            DataValue::Boolean(true),
            DataValue::String("text".to_string()),
            DataValue::I8(8),
        ];

        let mut data = BuilderData::from_properties(&header, values.clone()).build();

        let path = "test_read_by_positions.bin";

        assert!(data.write(path).is_ok());

        let property_headers_original_positions = vec![1, 2, 3];

        assert!(data
            .read_by_positions(path, &property_headers_original_positions)
            .is_ok());

        let expected =  vec![
            DataValue::Boolean(true),
            DataValue::I8(8),
            DataValue::String("text".to_string()),
        ];

        assert_eq!(expected, *data.get_values());

        fs::remove_file(path).unwrap();
    }
}
