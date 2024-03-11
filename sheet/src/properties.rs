use byteorder::ReadBytesExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, Write};

use crate::header::{DataType, Schema, PropertySchema};
use crate::{
    th, th_msg, th_none, Error, DEFAULT_SIZE_U32, FALSE_BIN_VALUE, NULL_BIN_VALUE, TRUE_BIN_VALUE,
};

#[derive(Debug, PartialEq, Clone)]
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

impl Data {
    pub fn to_string(&self) -> String {
        match self {
            Data::Null => "NULL".to_string(),
            Data::Boolean(value) => value.to_string(),
            Data::String(value) => value.to_string(),
            Data::U8(value) => value.to_string(),
            Data::U16(value) => value.to_string(),
            Data::U32(value) => value.to_string(),
            Data::U64(value) => value.to_string(),
            Data::U128(value) => value.to_string(),
            Data::I8(value) => value.to_string(),
            Data::I16(value) => value.to_string(),
            Data::I32(value) => value.to_string(),
            Data::I64(value) => value.to_string(),
            Data::I128(value) => value.to_string(),
            Data::F32(value) => value.to_string(),
            Data::F64(value) => value.to_string(),
        }
    }
}

/// Builder for Data struct
/// # Example
/// ```
/// let schema = Schema::from(vec![
///   ("varchar", DataType::Varchar(30)),
///   ("boolean", DataType::Boolean),
///   ("text", DataType::Text),
///   ("i8", DataType::I8),
/// ]);
///
/// let values = vec![
///   Data::String(format!("{: <30}", "varchar")),
///   Data::Boolean(true),
///   Data::String("text".to_string()),
///   Data::I8(8),
/// ];
///
/// let builder = BuilderProperties::from_properties(&header, "values.bin", values);
/// let data = builder.build();
/// ```
pub struct BuilderProperties<'a> {
    schema: &'a Schema,
    properties: Vec<Data>,
    dynamic_values: Vec<Data>,
}

impl<'a> BuilderProperties<'a> {
    /// Create a new BuilderProperties
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            properties: Vec::with_capacity(schema.len()),
            dynamic_values: Vec::new(),
        }
    }

    /// Create a new BuilderProperties from properties without orderer values.
    /// This method is unsafe because it does not check the order of the values.
    pub fn from_properties_unsafe(header: &'a Schema, properties: Vec<Data>) -> Properties<'a> {
        let builder = {
            let mut builder = Self::new(header);

            builder.properties = properties;

            builder
        };

        builder.build()
    }

    /// Create a new BuilderProperties from properties, ordering the values by the header
    pub fn from_properties(header: &'a Schema, values: Vec<Data>) -> Properties<'a> {
        let builder: BuilderProperties<'_> = {
            let mut builder = Self::new(header);

            for value in values {
                builder.add(value);
            }

            builder
        };

        builder.build()
    }

    /// Add a property to the builder
    pub fn add(&mut self, value: Data) {
        let position = self.properties.len() + self.dynamic_values.len();
        let prop = self.schema.get_by_original_position(position).unwrap();

        if prop.is_dynamic_size() {
            self.dynamic_values.push(value);
        } else {
            self.properties.push(value);
        }
    }

    /// Build the Data struct
    pub fn build(self) -> Properties<'a> {
        let mut properties: Vec<Data> = self.properties.clone();
        properties.append(&mut self.dynamic_values.clone());

        Properties {
            header: self.schema,
            properties,
        }
    }
}

/// Data struct
/// # Example
/// ```
/// use std::fs;
/// use sheet::{DataType, Data, Schema, BuilderProperties};
///
/// let schema = Schema::from(vec![
///   ("varchar", DataType::Varchar(30)),
///   ("boolean", DataType::Boolean),
///   ("text", DataType::Text),
///   ("i8", DataType::I8),
/// ]);
///
/// let values = vec![
///   Data::String(format!("{: <30}", "varchar")),
///   Data::Boolean(true),
///   Data::String("text".to_string()),
///   Data::I8(8),
/// ];
///
/// let mut properties = BuilderProperties::from_properties(&header, values).build();
///
/// let path = "test_data_struct.bin";
///
/// assert!(properties.write(path).is_ok());
///
/// assert!(properties.read(path).is_ok());
///
/// assert_eq!(values, *properties.get_properties());
///
/// fs::remove(path).unwrap();
/// ```
#[derive(Debug)]
pub struct Properties<'a> {
    header: &'a Schema,
    properties: Vec<Data>,
}

impl<'a> Properties<'a> {
    /// Create a new Data struct
    pub fn new(header: &'a Schema) -> Self {
        Self {
            header,
            properties: Vec::with_capacity(header.len()),
        }
    }

    pub fn get_sort_key_property(&self) -> &Data {
        let position = self.header.get_sort_key_position();

        match self.properties.get(position) {
            Some(value) => value,
            None => panic!("Sort key not found"),
        }
    }

    /// Write the Data struct to a file
    pub fn write(&mut self, path: &str) -> Result<(), Error> {
        let mut buffer_writer = BufWriter::new(th_msg!(File::create(path), Error::Io));

        if let Err(err) = write_properties(&mut buffer_writer, self.header, &self.properties) {
            return Err(err);
        }

        th!(buffer_writer.flush(), Error::WriteProperties);

        Ok(())
    }

    /// Read the Data struct from a file
    pub fn read(&mut self, path: &str) -> Result<(), Error> {
        let mut buffer_reader = BufReader::new(th_msg!(File::open(path), Error::Io));
        self.properties = read_properties(&mut buffer_reader, self.header)?;
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

        self.properties =
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

        self.properties =
            read_properties_by_byte_position(&mut buffer_reader, self.header, &property_headers)?;
        Ok(())
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&Data> {
        self.properties.get(index)
    }

    /// Get the values
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// Get the values
    pub fn get_by_label(&self, label: &[u8]) -> Result<&Data, Error> {
        let label = th!(self.header.get_by_label(label), Error::LabelNotFound);
        let label = th_none!(
            self.properties.get(label.get_position()),
            Error::LabelNotFound
        );
        Ok(label)
    }

    /// Get the values
    pub fn get_properties(&self) -> &Vec<Data> {
        &self.properties
    }

    /// Order the values by the original order
    pub fn get_properties_original_position(&self) -> Vec<&Data> {
        let mut new_order = Vec::new();

        for index in 0..self.header.len() {
            let prop = self.header.get_by_original_position(index).unwrap();
            let data = self.properties.get(prop.get_position()).unwrap();
            new_order.push(data);
        }

        new_order
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
                Data::$data_type(value)
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
                Data::$data_type(value)
            }
            _ => return Err(Error::ReadInvalidDataType),
        }
    }};
}

/// Write properties to a file
/// # Example
/// ```
/// let buffer_writer = &mut BufWriter::new(File::create("values.bin").unwrap());
/// let schema = vec![
///    PropertySchema::from(("varchar", DataType::Varchar(30))),
///    PropertySchema::from(("boolean", DataType::Boolean)),
///    PropertySchema::from(("text", DataType::Text)),
///    PropertySchema::from(("i8", DataType::I8)),
///    PropertySchema::from(("i16", DataType::I16)),
///    PropertySchema::from(("i32", DataType::I32)),
///    PropertySchema::from(("i64", DataType::I64)),
///    PropertySchema::from(("i128", DataType::I128)),
///    PropertySchema::from(("u8", DataType::U8)),
///    PropertySchema::from(("u16", DataType::U16)),
///    PropertySchema::from(("u32", DataType::U32)),
///    PropertySchema::from(("u64", DataType::U64)),
///    PropertySchema::from(("f32", DataType::F32)),
///    PropertySchema::from(("f64", DataType::F64)),
/// ];
/// let values = vec![
///    Data::String(format!("{: <30}", "varchar")),
///    Data::Boolean(true),
///    Data::String("text".to_string()),
///    Data::I8(8),
///    Data::I16(16),
///    Data::I32(32),
///    Data::I64(64),
///    Data::I128(128),
///    Data::U8(8),
///    Data::U16(16),
///    Data::U32(32),
///    Data::U64(64),
///    Data::F32(32.0),
///    Data::F64(64.0),
/// ];
///
/// write_properties(buffer_writer, &header, &values).unwrap();
/// buffer_writer.flush().unwrap();
/// ```
/// Properties pattern:
/// | data_type (only Text) | value | data_type (only Text) | value | ...
pub fn write_properties(
    buffer_writer: &mut BufWriter<File>,
    header: &Schema,
    values: &Vec<Data>,
) -> Result<(), Error> {
    for prop in header.headers_iter() {
        let value = th_none!(
            values.get(prop.get_position()),
            Error::WritePropertiesGetHeader
        );

        match value {
            Data::Boolean(data) => {
                th_msg!(
                    buffer_writer.write_all(&[if *data {
                        TRUE_BIN_VALUE
                    } else {
                        FALSE_BIN_VALUE
                    }]),
                    Error::Io
                );
            }
            Data::String(data) => match prop.get_data_type() {
                DataType::Varchar(size) => {
                    let size = *size;

                    if data.len() > size as usize {
                        return Err(Error::VarcharSize);
                    }

                    let mut buffer = vec![0; size as usize];
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
                        "Data is String, but Schema is {}",
                        prop.get_data_type()
                    )))
                }
            },
            Data::U8(data) => {
                write_data!(buffer_writer, data, prop, U8);
            }
            Data::U16(data) => {
                write_data!(buffer_writer, data, prop, U16);
            }
            Data::U32(data) => {
                write_data!(buffer_writer, data, prop, U32);
            }
            Data::U64(data) => {
                write_data!(buffer_writer, data, prop, U64);
            }
            Data::U128(data) => {
                write_data!(buffer_writer, data, prop, U128);
            }
            Data::I8(data) => {
                write_data!(buffer_writer, data, prop, I8);
            }
            Data::I16(data) => {
                write_data!(buffer_writer, data, prop, I16);
            }
            Data::I32(data) => {
                write_data!(buffer_writer, data, prop, I32);
            }
            Data::I64(data) => {
                write_data!(buffer_writer, data, prop, I64);
            }
            Data::I128(data) => {
                write_data!(buffer_writer, data, prop, I128);
            }
            Data::F32(data) => {
                write_data!(buffer_writer, data, prop, F32);
            }
            Data::F64(data) => {
                write_data!(buffer_writer, data, prop, F64);
            }
            Data::Null => {
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
/// let schema = vec![
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
///    Data::String(format!("{: <30}", "varchar")),
///    Data::Boolean(true),
///    Data::String("text".to_string()),
///    Data::I8(8),
///    Data::I16(16),
///    Data::I32(32),
///    Data::I64(64),
///    Data::I128(128),
///    Data::U8(8),
///    Data::U16(16),
///    Data::U32(32),
///    Data::U64(64),
///    Data::F32(32.0),
///    Data::F64(64.0),
/// ];
///
/// let read_properties = read_properties(buffer_reader, &header).unwrap();
/// assert_eq!(values, read_properties);
/// ```
pub fn read_properties(
    buffer_reader: &mut BufReader<File>,
    header: &Schema,
) -> Result<Vec<Data>, Error> {
    let mut values = Vec::with_capacity(header.len());

    for prop in header.headers_iter() {
        let value = match prop.get_data_type() {
            DataType::Varchar(size) => {
                let size = *size;
                let mut buffer = vec![0; size as usize];

                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                Data::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Text => {
                let size = th_msg!(
                    buffer_reader.read_u32::<byteorder::LittleEndian>(),
                    Error::Io
                ) as usize;

                let mut buffer = vec![0; size];

                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                Data::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
            }
            DataType::Boolean => {
                let value = th_msg!(buffer_reader.read_u8(), Error::Io);
                Data::Boolean(value == 1)
            }
            DataType::I8 => {
                let value = th_msg!(buffer_reader.read_i8(), Error::Io);
                Data::I8(value)
            }
            DataType::I16 => read_data!(buffer_reader, prop, I16, read_i16),
            DataType::I32 => read_data!(buffer_reader, prop, I32, read_i32),
            DataType::I64 => read_data!(buffer_reader, prop, I64, read_i64),
            DataType::I128 => read_data!(buffer_reader, prop, I128, read_i128),
            DataType::U8 => {
                let value = th_msg!(buffer_reader.read_u8(), Error::Io);
                Data::U8(value)
            }
            DataType::U16 => read_data!(buffer_reader, prop, U16, read_u16),
            DataType::U32 => read_data!(buffer_reader, prop, U32, read_u32),
            DataType::U64 => read_data!(buffer_reader, prop, U64, read_u64),
            DataType::U128 => read_data!(buffer_reader, prop, U128, read_u128),
            DataType::F32 => read_data!(buffer_reader, prop, F32, read_f32),
            DataType::F64 => read_data!(buffer_reader, prop, F64, read_f64),
            DataType::Null => Data::Null,
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
    header: &Schema,
    property_headers: &Vec<&PropertySchema>,
) -> Result<Vec<Data>, Error> {
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
                let mut buffer = vec![0; size as usize];

                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                Data::String(String::from_utf8(buffer).expect("UTF-8 decoding error"))
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
                                let mut buffer = vec![0; size];

                                th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

                                break Data::String(
                                    String::from_utf8(buffer).expect("UTF-8 decoding error"),
                                );
                            } else {
                                last_pos += (size + DEFAULT_SIZE_U32) as u64;
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
                Data::Boolean(value == 1)
            }
            DataType::I8 => {
                let pos = prop.get_byte_position().unwrap();
                th_msg!(
                    buffer_reader.seek(std::io::SeekFrom::Start(pos as u64)),
                    Error::Io
                );

                let value = th_msg!(buffer_reader.read_i8(), Error::Io);
                Data::I8(value)
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
                Data::U8(value)
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
            DataType::Null => Data::Null,
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
        let schema = Schema::try_from(vec![
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
        ])
        .unwrap();

        let values = vec![
            Data::String(format!("{: <30}", "varchar")),
            Data::Boolean(true),
            Data::I8(8),
            Data::I16(16),
            Data::I32(32),
            Data::I64(64),
            Data::I128(128),
            Data::U8(8),
            Data::U16(16),
            Data::U32(32),
            Data::U64(64),
            Data::F32(32.0),
            Data::F64(64.0),
            // Text is positioned at the end automatically because it is a dynamic size type
            Data::String("text".to_string()),
        ];

        let path: &str = "test_write_and_read_properties.bin";

        let buffer_writer = &mut BufWriter::new(File::create(path).unwrap());

        write_properties(buffer_writer, &schema, &values).unwrap();

        buffer_writer.flush().unwrap();

        let buffer_reader = &mut BufReader::new(File::open(path).unwrap());

        let read_properties = read_properties(buffer_reader, &schema).unwrap();
        assert_eq!(values, read_properties);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_data_struct() {
        let schema = Schema::try_from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ])
        .unwrap();

        let values = vec![
            Data::String(format!("{: <30}", "varchar")),
            Data::Boolean(true),
            Data::String("text".to_string()),
            Data::I8(8),
        ];

        let values_new_order = vec![
            Data::String(format!("{: <30}", "varchar")),
            Data::Boolean(true),
            Data::I8(8),
            Data::String("text".to_string()),
        ];

        let mut data = BuilderProperties::from_properties(&schema, values);

        let path = "test_data_struct.bin";

        assert!(data.write(path).is_ok());

        assert!(data.read(path).is_ok());

        assert_eq!(values_new_order, *data.get_properties());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_data_struct_unsafe() {
        let schema = Schema::try_from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ])
        .unwrap();

        let values = vec![
            Data::String(format!("{: <30}", "varchar")),
            Data::Boolean(true),
            Data::I8(8),
            Data::String("text".to_string()),
        ];
        let mut data = BuilderProperties::from_properties_unsafe(&schema, values.clone());

        let path = "test_data_struct_unsafe.bin";

        assert!(data.write(path).is_ok());

        assert!(data.read(path).is_ok());

        assert_eq!(values, *data.get_properties());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_by_original_positions() {
        let schema = Schema::try_from(vec![
            ("text", DataType::Text),
            ("varchar", DataType::Varchar(30)),
            ("text", DataType::Text),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ])
        .unwrap();

        let values = vec![
            Data::String("text0".to_string()),
            Data::String(format!("{: <30}", "varchar")),
            Data::String("text1".to_string()),
            Data::Boolean(true),
            Data::String("text2".to_string()),
            Data::I8(8),
        ];

        let mut data = BuilderProperties::from_properties(&schema, values.clone());

        let path = "test_read_by_original_positions.bin";

        assert!(data.write(path).is_ok());

        let property_headers_original_positions = vec![0, 1, 3, 2];

        assert!(data
            .read_by_original_positions(path, &property_headers_original_positions)
            .is_ok());

        let expected = property_headers_original_positions
            .iter()
            .map(|position| values.get(*position).unwrap().clone())
            .collect::<Vec<_>>();

        assert_eq!(expected, *data.get_properties());

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_read_by_positions() {
        let schema = Schema::try_from(vec![
            ("varchar", DataType::Varchar(30)),
            ("boolean", DataType::Boolean),
            ("text", DataType::Text),
            ("i8", DataType::I8),
        ])
        .unwrap();

        let values = vec![
            Data::String(format!("{: <30}", "varchar")),
            Data::Boolean(true),
            Data::String("text".to_string()),
            Data::I8(8),
        ];

        let mut data = BuilderProperties::from_properties(&schema, values.clone());

        let path = "test_read_by_positions.bin";

        assert!(data.write(path).is_ok());

        let property_headers_original_positions = vec![1, 2, 3];

        assert!(data
            .read_by_positions(path, &property_headers_original_positions)
            .is_ok());

        let expected = vec![
            Data::Boolean(true),
            Data::I8(8),
            Data::String("text".to_string()),
        ];

        assert_eq!(expected, *data.get_properties());

        fs::remove_file(path).unwrap();
    }
}
