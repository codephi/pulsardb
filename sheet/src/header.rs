use byteorder::ReadBytesExt;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Iter;

use crate::{
    th, Error, DATA_TYPE_BOOLEAN, DATA_TYPE_F32, DATA_TYPE_F64, DATA_TYPE_I128, DATA_TYPE_I16,
    DATA_TYPE_I32, DATA_TYPE_I64, DATA_TYPE_I8, DATA_TYPE_NULL, DATA_TYPE_TEXT, DATA_TYPE_U128,
    DATA_TYPE_U16, DATA_TYPE_U32, DATA_TYPE_U64, DATA_TYPE_U8, DATA_TYPE_UNDEFINED,
    DATA_TYPE_VARCHAR,
};

#[derive(Debug, PartialEq, Clone)]
pub enum DataType {
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
pub struct PropertyHeader {
    pub(crate) data_type: DataType,
    pub(crate) label: Vec<u8>,
    pub(crate) order: usize,
}

impl PropertyHeader {
    pub fn new(label: Vec<u8>, order: usize, data_type: DataType) -> Self {
        PropertyHeader {
            data_type,
            label: label.into(),
            order,
        }
    }
}

impl PropertyHeader {
    pub fn default_size(&self) -> usize {
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
        }
    }
}

/// Header struct
/// # Example
/// ```
/// let properties = vec![
///     PropertyHeader::from((DataType::I32, "age")),
///     PropertyHeader::from((DataType::Varchar(10), "name")),
///     PropertyHeader::from((DataType::F64, "height")),
///   ];
///
/// let header = Header::from(properties);
/// ```
#[derive(Debug, PartialEq)]
pub struct Header {
    headers: Vec<PropertyHeader>,
    sizes: Vec<usize>,
    is_dynamic_size: bool,
}

impl Header {
    pub fn new() -> Self {
        Header {
            headers: Vec::new(),
            sizes: Vec::new(),
            is_dynamic_size: false,
        }
    }

    pub fn headers_iter(&self) -> std::slice::Iter<'_, PropertyHeader>  {
        self.headers.iter()
    }

    pub fn add_property(&mut self, data_type: DataType, label: &str) {
        let property =
            PropertyHeader::new(label.as_bytes().to_vec(), self.headers.len(), data_type);
        self.add(property);
    }

    pub fn add(&mut self, property: PropertyHeader) {
        let size = property.default_size();
        self.headers.push(property);
        self.sizes.push(size);
    }

    pub fn get(&self, index: usize) -> Option<&PropertyHeader> {
        self.headers.get(index)
    }
}

impl From<Vec<PropertyHeader>> for Header {
    fn from(headers: Vec<PropertyHeader>) -> Self {
        let mut is_dynamic_size = false;

        let sizes = headers
            .iter()
            .map(|prop| {
                if !is_dynamic_size && prop.data_type == DataType::Text {
                    is_dynamic_size = true;
                }

                prop.default_size()
            })
            .collect();

        Header {
            headers,
            sizes,
            is_dynamic_size,
        }
    }
}

impl From<Vec<(&str, DataType)>> for Header {
    fn from(headers: Vec<(&str, DataType)>) -> Self {
        let mut sizes = Vec::new();
        let mut is_dynamic_size = false;

        let headers = headers
            .iter()
            .enumerate()
            .map(|(index, (label, data_type))| {
                let prop = PropertyHeader::new(label.as_bytes().to_vec(), index, data_type.clone());
                sizes.push(prop.default_size());

                if !is_dynamic_size && data_type == &DataType::Text {
                    is_dynamic_size = true;
                }

                prop
            })
            .collect();

        Header {
            headers,
            sizes,
            is_dynamic_size,
        }
    }
}

/// Write header to file
/// # Example
/// ```
/// let buffer_writer = &mut BufWriter::new(File::create("header.bin").unwrap());
/// let properties = vec![
///     PropertyHeader::from((DataType::I32, "age")),
///     PropertyHeader::from((DataType::Varchar(10), "name")),
///     PropertyHeader::from((DataType::F64, "height")),
///   ];
///   write_header(buffer_writer, &properties).unwrap();
/// ```
/// Header pattern:
/// | data_type | label_size | label | data_type | label_size | label |
// TODO: determitar tamanho fixo para a label
pub fn write_header(buffer_writer: &mut BufWriter<File>, properties: &Header) -> Result<(), Error> {
    for prop in properties.headers.iter() {
        let data_type_byte = prop.data_type.clone().into();
        // Write data type
        th!(buffer_writer.write_all(&[data_type_byte]), Error::Io);

        if let DataType::Varchar(size) = prop.data_type {
            th!(buffer_writer.write_all(&size.to_le_bytes()), Error::Io);
        }

        // Write label size
        th!(
            buffer_writer.write_all(&(prop.label.len() as u32).to_le_bytes()),
            Error::Io
        );
        // Write label
        th!(buffer_writer.write_all(&prop.label), Error::Io);
    }

    Ok(())
}

/// Read header from file
/// # Example
/// ```
/// let buffer_reader = &mut BufReader::new(File::open("header.bin").unwrap());
/// let properties = read_header(buffer_reader).unwrap();
/// ```
pub fn read_header(buffer: &mut BufReader<File>) -> Result<Header, Error> {
    let mut properties = Vec::new();

    while let Ok(data_type_byte) = buffer.read_u8() {
        let data_type = {
            if data_type_byte == DATA_TYPE_UNDEFINED {
                return Err(Error::ReadInvalidDataType);
            }

            if data_type_byte == DATA_TYPE_VARCHAR {
                let size = th!(buffer.read_u32::<byteorder::LittleEndian>(), Error::Io);
                DataType::Varchar(size)
            } else {
                DataType::from(data_type_byte)
            }
        };

        let label_size = th!(buffer.read_u32::<byteorder::LittleEndian>(), Error::Io);

        let mut label_bytes = vec![0u8; label_size as usize];

        th!(buffer.read_exact(&mut label_bytes), Error::Io);

        properties.push(PropertyHeader::new(
            label_bytes,
            properties.len(),
            data_type,
        ));
    }

    Ok(Header::from(properties))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_write_and_reader_header() {
        let header_path = "header.bin";
        let original_properties = Header::from(vec![
            ("name", DataType::Varchar(10)),
            ("age", DataType::I32),
            ("height", DataType::F64),
        ]);

        let buffer_writer = &mut BufWriter::new(File::create(header_path).unwrap());

        write_header(buffer_writer, &original_properties).unwrap();

        buffer_writer.flush().unwrap();

        let buffer_reader = &mut BufReader::new(File::open(header_path).unwrap());

        let properties = read_header(buffer_reader).unwrap();

        assert_eq!(properties, original_properties);

        fs::remove_file("header.bin").unwrap();
    }
}

// FGTS/{cpf}/{create_at}.bin
// [Varchar(30), "cpf", Varchar(30), "create_at"]
// 3cpf3create_at
