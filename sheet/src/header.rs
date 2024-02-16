use crate::{
    th, th_msg, Error, DATA_TYPE_BOOLEAN, DATA_TYPE_F32, DATA_TYPE_F64, DATA_TYPE_I128,
    DATA_TYPE_I16, DATA_TYPE_I32, DATA_TYPE_I64, DATA_TYPE_I8, DATA_TYPE_NULL, DATA_TYPE_TEXT,
    DATA_TYPE_U128, DATA_TYPE_U16, DATA_TYPE_U32, DATA_TYPE_U64, DATA_TYPE_U8, DATA_TYPE_UNDEFINED,
    DATA_TYPE_VARCHAR,
};
use byteorder::ReadBytesExt;
use core::fmt::Display;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};

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

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data_type = match self {
            DataType::Null => "Null",
            DataType::Boolean => "Boolean",
            DataType::Text => "Text",
            DataType::Varchar(_) => "Varchar",
            DataType::Undefined => "Undefined",
            DataType::I8 => "I8",
            DataType::I16 => "I16",
            DataType::I32 => "I32",
            DataType::I64 => "I64",
            DataType::I128 => "I128",
            DataType::U8 => "U8",
            DataType::U16 => "U16",
            DataType::U32 => "U32",
            DataType::U64 => "U64",
            DataType::U128 => "U128",
            DataType::F32 => "F32",
            DataType::F64 => "F64",
        };

        write!(f, "{}", data_type)
    }
}

impl DataType {
    pub fn is_dynamic_size(&self) -> bool {
        match self {
            DataType::Text => true,
            _ => false,
        }
    }
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

#[derive(Debug, PartialEq, Clone)]
pub struct PropertyHeader {
    data_type: DataType,
    label: Vec<u8>,
    position: usize,
    original_position: usize,
}

impl PropertyHeader {
    pub fn new(
        label: Vec<u8>,
        position: usize,
        original_position: usize,
        data_type: DataType,
    ) -> Self {
        PropertyHeader {
            data_type,
            label: label.into(),
            position,
            original_position,
        }
    }

    pub fn update_position(&mut self, position: usize) {
        self.position = position;
    }

    pub fn is_dynamic_size(&self) -> bool {
        self.data_type.is_dynamic_size()
    }

    pub fn get_position(&self) -> usize {
        self.position
    }

    pub fn get_original_position(&self) -> usize {
        self.original_position
    }

    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
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

/// BuilderHeader struct
/// # Example
/// ```
/// let builder = BuilderHeader::new();
/// builder.add("age", DataType::I32);
/// builder.add("name", DataType::Varchar(10));
/// builder.add("height", DataType::F64);
/// let header = builder.build();
/// ```
#[derive(Debug)]
pub struct BuilderHeader {
    headers: Vec<PropertyHeader>,
    headers_dynamic_size: Vec<PropertyHeader>,
    is_dynamic_size: bool,
    sizes: Vec<usize>,
}

impl BuilderHeader {
    /// Create a new BuilderHeader
    pub fn new() -> Self {
        Self {
            headers: Vec::new(),
            headers_dynamic_size: Vec::new(),
            is_dynamic_size: false,
            sizes: Vec::new(),
        }
    }

    /// Add a new property to the header
    pub fn add(&mut self, label: Vec<u8>, data_type: DataType) {
        let is_dynamic_size = data_type.is_dynamic_size();
        let (position, original_position) = if is_dynamic_size {
            (
                self.headers_dynamic_size.len(),
                self.headers_dynamic_size.len() + self.headers.len(),
            )
        } else {
            (
                self.headers.len(),
                self.headers_dynamic_size.len() + self.headers.len(),
            )
        };
        let prop = PropertyHeader::new(label, position, original_position, data_type);

        self.sizes.push(prop.default_size());

        if is_dynamic_size {
            if !self.is_dynamic_size {
                self.is_dynamic_size = true;
            }

            self.headers_dynamic_size.push(prop);
        } else {
            self.headers.push(prop);
        }
    }

    /// Build the header
    pub fn build(&mut self) -> Header {
        let headers_dynamic_size = &mut self
            .headers_dynamic_size
            .iter()
            .map(|prop| {
                let mut prop = prop.clone();
                prop.update_position(self.headers.len() + prop.get_position());
                prop
            })
            .collect::<Vec<_>>();

        let mut headers = self.headers.clone();
        headers.append(headers_dynamic_size);

        Header {
            headers,
            sizes: self.sizes.clone(),
            is_dynamic_size: self.is_dynamic_size,
        }
    }
}

/// BuilderHeader struct
/// # Example
/// ```
/// let builder = Header::from(vec![
///     ("age", DataType::I32),
///     ("name", DataType::Varchar(10)),
///     ("height", DataType::F64),
///   ]);
/// ```
#[derive(Debug)]
pub struct Header {
    headers: Vec<PropertyHeader>,
    sizes: Vec<usize>,
    is_dynamic_size: bool,
}

impl Header {
    /// Create a new Header
    pub fn new() -> Self {
        Header {
            headers: Vec::new(),
            sizes: Vec::new(),
            is_dynamic_size: false,
        }
    }

    pub fn len(&self) -> usize {
        self.headers.len()
    }

    /// Get the size of the header
    pub fn headers_iter(&self) -> std::slice::Iter<'_, PropertyHeader> {
        self.headers.iter()
    }

    /// Get the size of the header
    pub fn get(&self, index: usize) -> Option<&PropertyHeader> {
        self.headers.get(index)
    }

    pub fn get_by_original_position(&self, original_position: usize) -> Option<&PropertyHeader> {
        self.headers
            .iter()
            .find(|prop| prop.get_original_position() == original_position)
    }

    pub fn get_by_label(&self, label: &[u8]) -> Result<&PropertyHeader, Error> {
        match self.headers.iter().find(|prop| prop.label == label) {
            Some(prop) => Ok(prop),
            None => Err(Error::LabelNotFound),
        }
    }

    pub fn write(&mut self, path: &str) -> Result<(), Error> {
        let headers = self.headers_iter();
        let mut buffer_writer = BufWriter::new(File::create(path).unwrap());

        if let Err(err) = write_header(&mut buffer_writer, headers) {
            return Err(err);
        }

        th!(buffer_writer.flush(), Error::WriteProperties);

        Ok(())
    }

    pub fn read(&mut self, path: &str) -> Result<(), Error> {
        let mut is_dynamic_size = self.is_dynamic_size;
        let mut buffer_reader = BufReader::new(File::open(path).unwrap());

        self.headers = read_header(&mut buffer_reader)?;

        self.sizes = self
            .headers
            .iter()
            .map(|prop| {
                let size = prop.default_size();
                if prop.is_dynamic_size() {
                    is_dynamic_size = true;
                }
                size
            })
            .collect();

        Ok(())
    }
}

/// Implement From for Header
/// # Example
/// ```
/// let header = Header::from(vec![
///     ("age", DataType::I32),
///     ("name", DataType::Varchar(10)),
///     ("height", DataType::F64),
///   ]);
/// ```
impl<'a> From<Vec<(&str, DataType)>> for Header {
    fn from(headers: Vec<(&str, DataType)>) -> Self {
        let mut buidler = BuilderHeader::new();

        headers.iter().for_each(|(label, data_type)| {
            buidler.add(label.as_bytes().to_vec(), data_type.clone())
        });

        buidler.build()
    }
}

/// Write header to file
/// # Example
/// ```
/// let buffer_writer = &mut BufWriter::new(File::create("header.bin").unwrap());
/// let header = Header::from(vec![
///     ("name", DataType::Varchar(10)),
///     ("age", DataType::I32),
///     ("height", DataType::F64),
///   ]);
///   write_header(buffer_writer, &header).unwrap();
/// ```
/// Header pattern:
/// | data_type | label_size | label | data_type | label_size | label |
// TODO: determitar tamanho fixo para a label
pub fn write_header(
    buffer_writer: &mut BufWriter<File>,
    headers: std::slice::Iter<'_, PropertyHeader>,
) -> Result<(), Error> {
    for prop in headers {
        let data_type_byte = prop.data_type.clone().into();
        // Write data type
        th_msg!(buffer_writer.write_all(&[data_type_byte]), Error::Io);

        if let DataType::Varchar(size) = prop.data_type {
            th_msg!(buffer_writer.write_all(&size.to_le_bytes()), Error::Io);
        }

        // Write label size
        th_msg!(
            buffer_writer.write_all(&(prop.label.len() as u32).to_le_bytes()),
            Error::Io
        );
        // Write label
        th_msg!(buffer_writer.write_all(&prop.label), Error::Io);
    }

    Ok(())
}

/// Read header from file
/// # Example
/// ```
/// let buffer_reader = &mut BufReader::new(File::open("header.bin").unwrap());
/// let properties = read_header(buffer_reader).unwrap();
/// ```
pub fn read_header(buffer_reader: &mut BufReader<File>) -> Result<Vec<PropertyHeader>, Error> {
    let mut properties = Vec::new();

    while let Ok(data_type_byte) = buffer_reader.read_u8() {
        let data_type = {
            if data_type_byte == DATA_TYPE_UNDEFINED {
                return Err(Error::ReadInvalidDataType);
            }

            if data_type_byte == DATA_TYPE_VARCHAR {
                let size = th_msg!(
                    buffer_reader.read_u32::<byteorder::LittleEndian>(),
                    Error::Io
                );
                DataType::Varchar(size)
            } else {
                DataType::from(data_type_byte)
            }
        };

        let label_size = th_msg!(
            buffer_reader.read_u32::<byteorder::LittleEndian>(),
            Error::Io
        );

        let mut label_bytes = vec![0u8; label_size as usize];

        th_msg!(buffer_reader.read_exact(&mut label_bytes), Error::Io);

        properties.push(PropertyHeader::new(
            label_bytes,
            properties.len(),
            properties.len(),
            data_type,
        ));
    }

    Ok(properties)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_write_and_reader_header() {
        let header_path = "header.bin";
        let original_properties = Header::from(vec![
            ("varchar", DataType::Varchar(10)),
            ("text", DataType::Text),
            ("i32", DataType::I32),
            ("f64", DataType::F64),
        ]);

        let varchar_prop = original_properties
            .get_by_label("varchar".as_bytes())
            .unwrap();
        let text_prop = original_properties.get_by_label("text".as_bytes()).unwrap();
        let i32_prop = original_properties.get_by_label("i32".as_bytes()).unwrap();
        let f64_prop = original_properties.get_by_label("f64".as_bytes()).unwrap();

        assert_eq!(varchar_prop.get_position(), 0);
        assert_eq!(varchar_prop.get_original_position(), 0);
        assert_eq!(text_prop.get_position(), 3);
        assert_eq!(text_prop.get_original_position(), 1);
        assert_eq!(i32_prop.get_position(), 1);
        assert_eq!(i32_prop.get_original_position(), 2);
        assert_eq!(f64_prop.get_position(), 2);
        assert_eq!(f64_prop.get_original_position(), 3);

        let buffer_writer = &mut BufWriter::new(File::create(header_path).unwrap());

        write_header(buffer_writer, original_properties.headers_iter()).unwrap();

        buffer_writer.flush().unwrap();

        let buffer_reader = &mut BufReader::new(File::open(header_path).unwrap());

        let properties = read_header(buffer_reader).unwrap();

        let original_properties_new_order = Header::from(vec![
            ("varchar", DataType::Varchar(10)),
            ("i32", DataType::I32),
            ("f64", DataType::F64),
            ("text", DataType::Text),
        ]);

        assert_eq!(properties, original_properties_new_order.headers);

        fs::remove_file("header.bin").unwrap();
    }

    #[test]
    fn test_header_builder() {
        let mut header = Header::from(vec![
            ("name", DataType::Varchar(10)),
            ("age", DataType::I32),
            ("height", DataType::F64),
        ]);

        let name_prop = header.get_by_label("name".as_bytes()).unwrap();
        let age_prop = header.get_by_label("age".as_bytes()).unwrap();
        let height_prop = header.get_by_label("height".as_bytes()).unwrap();

        assert_eq!(name_prop.get_position(), 0);
        assert_eq!(name_prop.get_original_position(), 0);
        assert_eq!(age_prop.get_position(), 1);
        assert_eq!(age_prop.get_original_position(), 1);
        assert_eq!(height_prop.get_position(), 2);
        assert_eq!(height_prop.get_original_position(), 2);
        let path = "header.bin";

        header.write(path).unwrap();

        let mut header = Header::new();

        header.read(path).unwrap();

        let name_prop = header.get_by_label("name".as_bytes()).unwrap();
        let age_prop = header.get_by_label("age".as_bytes()).unwrap();
        let height_prop = header.get_by_label("height".as_bytes()).unwrap();

        assert_eq!(name_prop.get_position(), 0);
        assert_eq!(name_prop.get_original_position(), 0);
        assert_eq!(age_prop.get_position(), 1);
        assert_eq!(age_prop.get_original_position(), 1);
        assert_eq!(height_prop.get_position(), 2);
        assert_eq!(height_prop.get_original_position(), 2);
    }
}
