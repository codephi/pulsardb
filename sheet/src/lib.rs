//! # Sheet
//!
//! Sheet is a library to read and write data to a binary file.
//! # Features
//! - Read and write header to a binary file
//! - Read and write properties to a binary file
//! - Read full binary properties file.
//! - Provides objective reading, capturing only the necessary properties.
//!

use std::io;

mod header;
mod macros;
mod properties;

pub use header::*;
pub use properties::*;

/// Data type for the sheet
pub const DATA_TYPE_UNDEFINED: u8 = 0;
pub const DATA_TYPE_NULL: u8 = 1;
pub const DATA_TYPE_BOOLEAN: u8 = 2;
pub const DATA_TYPE_VARCHAR: u8 = 3;
pub const DATA_TYPE_TEXT: u8 = 4;
pub const DATA_TYPE_U8: u8 = 5;
pub const DATA_TYPE_U16: u8 = 6;
pub const DATA_TYPE_U32: u8 = 7;
pub const DATA_TYPE_U128: u8 = 9;
pub const DATA_TYPE_U64: u8 = 8;
pub const DATA_TYPE_I8: u8 = 10;
pub const DATA_TYPE_I16: u8 = 11;
pub const DATA_TYPE_I32: u8 = 12;
pub const DATA_TYPE_I64: u8 = 13;
pub const DATA_TYPE_I128: u8 = 14;
pub const DATA_TYPE_F32: u8 = 15;
pub const DATA_TYPE_F64: u8 = 16;

/// Data type for the sheet
pub const NULL_BIN_VALUE: u8 = 0;
pub const FALSE_BIN_VALUE: u8 = 0;
pub const TRUE_BIN_VALUE: u8 = 1;

/// Data type for the sheet
pub const DEFAULT_SIZE_BOOLEAN: usize = 1;
pub const DEFAULT_SIZE_TEXT: usize = 0;
pub const DEFAULT_SIZE_NULL: usize = 0;
pub const DEFAULT_SIZE_I8: usize = 1;
pub const DEFAULT_SIZE_I16: usize = 2;
pub const DEFAULT_SIZE_I32: usize = 4;
pub const DEFAULT_SIZE_I64: usize = 8;
pub const DEFAULT_SIZE_I128: usize = 16;
pub const DEFAULT_SIZE_U8: usize = 1;
pub const DEFAULT_SIZE_U16: usize = 2;
pub const DEFAULT_SIZE_U32: usize = 4;
pub const DEFAULT_SIZE_U64: usize = 8;
pub const DEFAULT_SIZE_U128: usize = 16;
pub const DEFAULT_SIZE_F32: usize = 4;
pub const DEFAULT_SIZE_F64: usize = 8;
pub const DEFAULT_SIZE_UNDEFINED: usize = 0;
#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    WriteInvalidDataType,
    WriteInvalidDataTypeNumber,
    WriteInvalidDataTypeString(String),
    ReadInvalidDataType,
    VarcharSize,
    NumberParse,
    WriteProperties,
    WritePropertiesGetHeader,
    WriteHeader,
    LabelNotFound,
    NoGetBytePosition,
    LabelExists(String),
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn test_write_header_and_properties() {
        let header_path = "test_write_header_and_properties_header.bin";
        let properties_path = "test_write_header_and_properties_properties.bin";

        let mut builder_header = BuilderHeader::new();

        builder_header.add("name".into(), DataType::Varchar(4));
        builder_header.add("age".into(), DataType::U8);
        builder_header.add("description".into(), DataType::Text);
        builder_header.add("active".into(), DataType::Boolean);

        let mut header = builder_header.build();

        header.write(header_path).unwrap();

        let header_values = header.get_headers().clone();

        header.read(header_path).unwrap();

        assert_eq!(&header_values, header.get_headers());

        let mut builder_properties = BuilderProperties::new(&header);

        builder_properties.add(Data::String("John".into()));
        builder_properties.add(Data::U8(18));
        builder_properties.add(Data::String("This is a description".into()));
        builder_properties.add(Data::Boolean(true));

        let mut properties = builder_properties.build();

        properties.write(properties_path).unwrap();

        let properties_values = properties.get_properties().clone();

        properties.read(properties_path).unwrap();

        assert_eq!(&properties_values, properties.get_properties());

        fs::remove_file(header_path).unwrap();
        fs::remove_file(properties_path).unwrap();
    }

    #[test]
    fn test_read_properties() {
        let header_path = "test_read_properties_header.bin";
        let properties_path = "test_read_properties_properties.bin";

        let mut builder_header = BuilderHeader::new();

        builder_header.add("name".into(), DataType::Varchar(4));
        builder_header.add("age".into(), DataType::U8);
        builder_header.add("description".into(), DataType::Text);
        builder_header.add("active".into(), DataType::Boolean);

        let mut header = builder_header.build();

        header.write(header_path).unwrap();

        let header_values = header.get_headers().clone();

        header.read(header_path).unwrap();

        assert_eq!(&header_values, header.get_headers());

        let mut builder_properties = BuilderProperties::new(&header);

        builder_properties.add(Data::String("John".into()));
        builder_properties.add(Data::U8(18));
        builder_properties.add(Data::String("This is a description".into()));
        builder_properties.add(Data::Boolean(true));

        let mut properties = builder_properties.build();

        properties.write(properties_path).unwrap();

        let properties_values = properties.get_properties().clone();

        properties.read(properties_path).unwrap();

        assert_eq!(&properties_values, properties.get_properties());

        ////////////////////////////////////////////////////////////////////////
        // this test start here
        ////////////////////////////////////////////////////////////////////////

        let mut header = Header::new();
        header.read(header_path).unwrap();

        let mut properties = Properties::new(&header);
        properties.read(properties_path).unwrap();

        let properties_values = properties.get_properties_original_position().clone();

        assert_eq!(4, properties_values.len());
        assert_eq!(&Data::String("John".into()), properties_values[0]);
        assert_eq!(&Data::U8(18), properties_values[1]);
        assert_eq!(
            &Data::String("This is a description".into()),
            properties_values[2]
        );
        assert_eq!(&Data::Boolean(true), properties_values[3]);

        fs::remove_file(header_path).unwrap();
        fs::remove_file(properties_path).unwrap();
    }
}
