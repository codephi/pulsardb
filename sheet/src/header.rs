/// Schema
/// The header binary file is composed by:
///
/// | total_schemas | schema_size      | original_position | data_type | label_size | label  ...
/// |---------------|------------------|-------------------|-----------|------------|------- ...
/// | u32           | u32              | u32               | u8        | u32        | Vec    ...
///
/// - total_schemas: total of schemas in the header.
/// - schema_size: total of properties in the header.
/// - original_position: original position of the property.
/// - data_type: data type of the property.
/// - label_size: size of the label.
/// - label: label of the property.
use crate::{
    th, th_msg, Error, DATA_TYPE_BOOLEAN, DATA_TYPE_F32, DATA_TYPE_F64, DATA_TYPE_I128,
    DATA_TYPE_I16, DATA_TYPE_I32, DATA_TYPE_I64, DATA_TYPE_I8, DATA_TYPE_NULL, DATA_TYPE_TEXT,
    DATA_TYPE_U128, DATA_TYPE_U16, DATA_TYPE_U32, DATA_TYPE_U64, DATA_TYPE_U8, DATA_TYPE_UNDEFINED,
    DATA_TYPE_VARCHAR, DEFAULT_SIZE_BOOLEAN, DEFAULT_SIZE_F32, DEFAULT_SIZE_F64, DEFAULT_SIZE_I128,
    DEFAULT_SIZE_I16, DEFAULT_SIZE_I32, DEFAULT_SIZE_I64, DEFAULT_SIZE_I8, DEFAULT_SIZE_NULL,
    DEFAULT_SIZE_TEXT, DEFAULT_SIZE_U128, DEFAULT_SIZE_U16, DEFAULT_SIZE_U32, DEFAULT_SIZE_U64,
    DEFAULT_SIZE_U8, DEFAULT_SIZE_UNDEFINED,
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
pub struct PropertySchema {
    data_type: DataType,
    label: Vec<u8>,
    original_position: usize,
    position: usize,
    byte_position: Option<usize>,
}

impl PropertySchema {
    pub fn new(
        label: Vec<u8>,
        original_position: usize,
        position: usize,
        data_type: DataType,
    ) -> Self {
        PropertySchema {
            data_type,
            label: label.into(),
            position,
            original_position,
            byte_position: None,
        }
    }

    pub fn update_position(&mut self, position: usize) {
        self.position = position;
    }

    pub fn update_byte_position(&mut self, byte_position: usize) {
        self.byte_position = Some(byte_position);
    }

    pub fn is_dynamic_size(&self) -> bool {
        self.data_type.is_dynamic_size()
    }

    pub fn get_position(&self) -> usize {
        self.position
    }

    pub fn get_byte_position(&self) -> Option<usize> {
        self.byte_position
    }

    pub fn get_original_position(&self) -> usize {
        self.original_position
    }

    pub fn get_data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl PropertySchema {
    pub fn default_size(&self) -> usize {
        match self.data_type {
            DataType::Varchar(size) => size as usize,
            DataType::Boolean => DEFAULT_SIZE_BOOLEAN,
            DataType::Text => DEFAULT_SIZE_TEXT,
            DataType::Null => DEFAULT_SIZE_NULL,
            DataType::I8 => DEFAULT_SIZE_I8,
            DataType::I16 => DEFAULT_SIZE_I16,
            DataType::I32 => DEFAULT_SIZE_I32,
            DataType::I64 => DEFAULT_SIZE_I64,
            DataType::I128 => DEFAULT_SIZE_I128,
            DataType::U8 => DEFAULT_SIZE_U8,
            DataType::U16 => DEFAULT_SIZE_U16,
            DataType::U32 => DEFAULT_SIZE_U32,
            DataType::U64 => DEFAULT_SIZE_U64,
            DataType::U128 => DEFAULT_SIZE_U128,
            DataType::F32 => DEFAULT_SIZE_F32,
            DataType::F64 => DEFAULT_SIZE_F64,
            DataType::Undefined => DEFAULT_SIZE_UNDEFINED,
        }
    }
}

/// BuilderSchema struct
/// # Example
/// ```
/// let builder = BuilderSchema::new();
/// builder.add("age", DataType::I32);
/// builder.add("name", DataType::Varchar(10));
/// builder.add("height", DataType::F64);
/// let schema = builder.build();
/// ```
#[derive(Debug)]
pub struct BuilderSchema {
    properties: Vec<PropertySchema>,
    headers_dynamic_size: Vec<PropertySchema>,
    is_dynamic_size: bool,
    next_byte_position: usize,
    dynamic_size_positions: Vec<usize>,
    sort_key_position: usize,
}

impl BuilderSchema {
    /// Create a new BuilderSchema
    pub fn new() -> Self {
        Self {
            properties: Vec::new(),
            headers_dynamic_size: Vec::new(),
            is_dynamic_size: false,
            next_byte_position: 0,
            dynamic_size_positions: Vec::new(),
            sort_key_position: 0,
        }
    }

    pub fn set_sort_key_position(&mut self, position: usize) {
        self.sort_key_position = position;
    }

    /// Add a new property to the header
    pub fn add(&mut self, label: Vec<u8>, data_type: DataType) -> Result<(), Error> {
        if self.properties.iter().any(|prop| prop.label == label) {
            return Err(Error::LabelExists(String::from_utf8(label).unwrap()));
        }

        let is_dynamic_size = data_type.is_dynamic_size();
        let (position, original_position) = if is_dynamic_size {
            (
                self.headers_dynamic_size.len(),
                self.headers_dynamic_size.len() + self.properties.len(),
            )
        } else {
            (
                self.properties.len(),
                self.headers_dynamic_size.len() + self.properties.len(),
            )
        };

        let mut prop = PropertySchema::new(label, original_position, position, data_type);

        if is_dynamic_size {
            if !self.is_dynamic_size {
                self.is_dynamic_size = true;
            }

            self.headers_dynamic_size.push(prop);
        } else {
            prop.update_byte_position(self.next_byte_position);

            self.next_byte_position += prop.default_size();

            self.properties.push(prop);
        }

        Ok(())
    }

    /// Build the header
    pub fn build(&mut self) -> Schema {
        let headers_dynamic_size = &mut self
            .headers_dynamic_size
            .iter()
            .map(|prop| {
                let mut prop = prop.clone();
                prop.update_position(self.properties.len() + prop.get_position());

                self.dynamic_size_positions.push(prop.get_position());

                prop
            })
            .collect::<Vec<_>>();

        let mut properties = self.properties.clone();
        properties.append(headers_dynamic_size);

        Schema {
            properties,
            is_dynamic_size: self.is_dynamic_size,
            last_byte_position_no_dynamic: if self.is_dynamic_size {
                Some(self.next_byte_position)
            } else {
                None
            },
            dynamic_size_positions: self.dynamic_size_positions.clone(),
            sort_key_position: self.sort_key_position,
        }
    }

    /// Exclusive use of crate! Add a new property to the header.
    pub(crate) fn add_property_raw(&mut self, mut prop: PropertySchema) -> Result<(), Error> {
        if prop.data_type.is_dynamic_size() {
            if !self.is_dynamic_size {
                self.is_dynamic_size = true;
            }

            self.headers_dynamic_size.push(prop);
        } else {
            prop.update_byte_position(self.next_byte_position);

            self.next_byte_position += prop.default_size();

            self.properties.push(prop);
        }

        Ok(())
    }

    /// Exclusive use of crate! Build the header.
    pub(crate) fn build_raw(&mut self) -> (Vec<PropertySchema>, bool, Option<usize>, Vec<usize>) {
        let headers_dynamic_size = &mut self
            .headers_dynamic_size
            .iter()
            .map(|prop| {
                let mut prop = prop.clone();
                prop.update_position(prop.get_position());

                self.dynamic_size_positions.push(prop.get_position());

                prop
            })
            .collect::<Vec<_>>();

        let mut properties = self.properties.clone();
        properties.append(headers_dynamic_size);

        (
            properties,
            self.is_dynamic_size,
            if self.is_dynamic_size {
                Some(self.next_byte_position)
            } else {
                None
            },
            self.dynamic_size_positions.clone(),
        )
    }
}

/// BuilderSchema struct
/// # Example
/// ```
/// let builder = Schema::from(vec![
///     ("age", DataType::I32),
///     ("name", DataType::Varchar(10)),
///     ("height", DataType::F64),
///   ]);
/// ```
#[derive(Debug)]
pub struct Schema {
    /// The properties of the header
    properties: Vec<PropertySchema>,
    /// If the schema has dynamic size
    is_dynamic_size: bool,
    /// The last byte position of the header without dynamic size
    last_byte_position_no_dynamic: Option<usize>,
    /// The positions of the dynamic size properties
    dynamic_size_positions: Vec<usize>,
    /// The position of the sort key
    sort_key_position: usize,
}

impl Schema {
    /// Create a new Schema
    pub fn new() -> Self {
        Self {
            properties: Vec::new(),
            is_dynamic_size: false,
            last_byte_position_no_dynamic: None,
            dynamic_size_positions: Vec::new(),
            sort_key_position: 0,
        }
    }

    pub fn get_sort_key(&mut self) -> DataType {
        self.properties[self.sort_key_position].data_type.clone()
    }

    pub fn get_sort_key_position(&self) -> usize {
        self.sort_key_position
    }

    pub fn get_sort_key_label(&self) -> &[u8] {
        &self.properties[self.sort_key_position].label
    }

    /// Create a new Schema
    pub fn len(&self) -> usize {
        self.properties.len()
    }

    /// Get the size of the header
    pub fn get_properties(&self) -> &Vec<PropertySchema> {
        &self.properties
    }

    /// Get the size of the header
    pub fn headers_iter(&self) -> std::slice::Iter<'_, PropertySchema> {
        self.properties.iter()
    }

    /// Get the size of the header
    pub fn get(&self, index: usize) -> Option<&PropertySchema> {
        self.properties.get(index)
    }

    /// Get the size of the header
    pub fn get_by_original_position(&self, original_position: usize) -> Option<&PropertySchema> {
        self.properties
            .iter()
            .find(|prop| prop.get_original_position() == original_position)
    }

    /// Get the size of the header
    pub fn get_last_byte_position_no_dynamic(&self) -> Option<usize> {
        self.last_byte_position_no_dynamic
    }

    /// Get the size of the header
    pub fn get_by_label(&self, label: &[u8]) -> Result<&PropertySchema, Error> {
        match self.properties.iter().find(|prop| prop.label == label) {
            Some(prop) => Ok(prop),
            None => Err(Error::LabelNotFound),
        }
    }

    /// Get the size of the header
    pub fn get_dynamic_size_positions(&self) -> Vec<usize> {
        self.dynamic_size_positions.clone()
    }

    /// Write header to file
    pub fn write(&mut self, path: &str) -> Result<(), Error> {
        let headers = self.headers_iter();
        let mut buffer_writer = BufWriter::new(File::create(path).unwrap());

        if let Err(err) = write_header(&mut buffer_writer, headers) {
            return Err(err);
        }

        th!(buffer_writer.flush(), Error::WriteProperties);

        Ok(())
    }

    /// Read header from file
    pub fn read(&mut self, path: &str) -> Result<(), Error> {
        let mut buffer_reader = BufReader::new(File::open(path).unwrap());

        let headers = read_header(&mut buffer_reader)?;

        let mut builder = BuilderSchema::new();

        for prop in headers {
            builder.add_property_raw(prop)?;
        }

        let schema = builder.build_raw();

        self.properties = schema.0;
        self.is_dynamic_size = schema.1;
        self.last_byte_position_no_dynamic = schema.2;
        self.dynamic_size_positions = schema.3;

        Ok(())
    }
}

/// Implement From for Schema
/// # Example
/// ```
/// let schema = Schema::from(vec![
///     ("age", DataType::I32),
///     ("name", DataType::Varchar(10)),
///     ("height", DataType::F64),
///   ]);
/// ```
impl<'a> TryFrom<Vec<(&str, DataType)>> for Schema {
    type Error = Error;

    fn try_from(headers: Vec<(&str, DataType)>) -> Result<Self, Self::Error> {
        let mut buidler = BuilderSchema::new();

        for (label, data_type) in headers {
            buidler.add(label.as_bytes().to_vec(), data_type)?;
        }

        Ok(buidler.build())
    }
}

/// Write header to file
/// # Example
/// ```
/// let buffer_writer = &mut BufWriter::new(File::create("header.bin").unwrap());
/// let schema = Schema::from(vec![
///     ("name", DataType::Varchar(10)),
///     ("age", DataType::I32),
///     ("height", DataType::F64),
///   ]);
///   write_header(buffer_writer, &header).unwrap();
/// ```
/// Schema pattern:
/// | data_type | label_size | label | data_type | label_size | label |
// TODO: determitar tamanho fixo para a label
pub fn write_header(
    buffer_writer: &mut BufWriter<File>,
    headers: std::slice::Iter<'_, PropertySchema>,
) -> Result<(), Error> {
    // Write total size
    th_msg!(
        buffer_writer.write_all(&(headers.len() as u32).to_le_bytes()),
        Error::Io
    );

    for prop in headers {
        let data_type_byte = prop.data_type.clone().into();

        // Write original position
        th_msg!(
            buffer_writer.write_all(&(prop.original_position as u32).to_le_bytes()),
            Error::Io
        );

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
pub fn read_header(buffer_reader: &mut BufReader<File>) -> Result<Vec<PropertySchema>, Error> {
    let schema_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    );
    let mut properties = Vec::with_capacity(schema_size as usize);

    while let Ok(original_position_byte) = buffer_reader.read_u32::<byteorder::LittleEndian>() {
        let original_position = original_position_byte as usize;
        let data_type_byte = th_msg!(buffer_reader.read_u8(), Error::Io);

        let data_type = match data_type_byte {
            DATA_TYPE_VARCHAR => {
                let size = th_msg!(
                    buffer_reader.read_u32::<byteorder::LittleEndian>(),
                    Error::Io
                );
                DataType::Varchar(size)
            }
            DATA_TYPE_UNDEFINED => return Err(Error::ReadInvalidDataType),
            _ => DataType::from(data_type_byte),
        };

        let label_size = th_msg!(
            buffer_reader.read_u32::<byteorder::LittleEndian>(),
            Error::Io
        );

        let mut label_bytes = vec![0; label_size as usize];

        th_msg!(buffer_reader.read_exact(&mut label_bytes), Error::Io);

        properties.push(PropertySchema::new(
            label_bytes,
            original_position,
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
        let original_properties = Schema::try_from(vec![
            ("varchar", DataType::Varchar(10)),
            ("text", DataType::Text),
            ("i32", DataType::I32),
            ("f64", DataType::F64),
        ])
        .unwrap();

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

        let path = "test_write_and_reader_header.bin";
        let buffer_writer = &mut BufWriter::new(File::create(path).unwrap());

        write_header(buffer_writer, original_properties.headers_iter()).unwrap();

        buffer_writer.flush().unwrap();

        let buffer_reader = &mut BufReader::new(File::open(path).unwrap());

        let properties = read_header(buffer_reader).unwrap();

        let original_properties_new_order = vec![
            PropertySchema::new("varchar".as_bytes().to_vec(), 0, 0, DataType::Varchar(10)),
            PropertySchema::new("i32".as_bytes().to_vec(), 2, 1, DataType::I32),
            PropertySchema::new("f64".as_bytes().to_vec(), 3, 2, DataType::F64),
            PropertySchema::new("text".as_bytes().to_vec(), 1, 3, DataType::Text),
        ];

        assert_eq!(properties, original_properties_new_order);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_header_builder() {
        let mut header = Schema::try_from(vec![
            ("varchar", DataType::Varchar(10)),
            ("text", DataType::Text),
            ("i32", DataType::I32),
            ("boolean", DataType::Boolean),
        ])
        .unwrap();

        let varchar_prop = header.get_by_label("varchar".as_bytes()).unwrap();
        let text_prop = header.get_by_label("text".as_bytes()).unwrap();
        let i32_prop = header.get_by_label("i32".as_bytes()).unwrap();
        let boolean_prop = header.get_by_label("boolean".as_bytes()).unwrap();

        assert_eq!(varchar_prop.get_position(), 0);
        assert_eq!(varchar_prop.get_original_position(), 0);
        assert_eq!(text_prop.get_position(), 3);
        assert_eq!(text_prop.get_original_position(), 1);
        assert_eq!(i32_prop.get_position(), 1);
        assert_eq!(i32_prop.get_original_position(), 2);
        assert_eq!(boolean_prop.get_position(), 2);
        assert_eq!(boolean_prop.get_original_position(), 3);

        let path = "test_header_builder.bin";
        assert!(header.write(path).is_ok());

        assert!(header.read(path).is_ok());

        let varchar_prop = header.get_by_label("varchar".as_bytes()).unwrap();
        let text_prop = header.get_by_label("text".as_bytes()).unwrap();
        let i32_prop = header.get_by_label("i32".as_bytes()).unwrap();
        let boolean_prop = header.get_by_label("boolean".as_bytes()).unwrap();

        assert_eq!(varchar_prop.get_position(), 0);
        assert_eq!(varchar_prop.get_original_position(), 0);
        assert_eq!(text_prop.get_position(), 3);
        assert_eq!(text_prop.get_original_position(), 1);
        assert_eq!(i32_prop.get_position(), 1);
        assert_eq!(i32_prop.get_original_position(), 2);
        assert_eq!(boolean_prop.get_position(), 2);
        assert_eq!(boolean_prop.get_original_position(), 3);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_bytes_position() {
        let schema = Schema::try_from(vec![
            ("varchar", DataType::Varchar(10)),
            ("text", DataType::Text),
            ("text2", DataType::Text),
            ("i32", DataType::I32),
            ("boolean", DataType::Boolean),
        ])
        .unwrap();

        assert_eq!(schema.properties.get(0).unwrap().get_byte_position(), Some(0));
        assert_eq!(schema.properties.get(1).unwrap().get_byte_position(), Some(10));
        assert_eq!(
            schema.properties.get(2).unwrap().get_byte_position(),
            Some(DEFAULT_SIZE_I32 + 10)
        );
        assert_eq!(schema.properties.get(3).unwrap().get_byte_position(), None);
        assert_eq!(schema.properties.get(4).unwrap().get_byte_position(), None);
        assert_eq!(schema.get_dynamic_size_positions(), vec![3, 4]);
    }

    #[test]
    fn test_label_exists() {
        let mut builder = BuilderSchema::new();

        assert!(builder
            .add("varchar".as_bytes().to_vec(), DataType::Varchar(10))
            .is_ok());
        assert!(builder
            .add("varchar".as_bytes().to_vec(), DataType::Varchar(10))
            .is_err());
    }
}
