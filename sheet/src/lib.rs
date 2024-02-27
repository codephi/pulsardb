//! # Symphony Sheet
//!
//! Sheet is a Rust library designed for efficient and safe reading and writing of data in binary files.
//! Focused on simplicity and performance, Sheet enables developers to handle complex data structures
//! persisted on disk, optimizing for both read and write operations.
//!
//! ## Key Features
//!
//! - **Header and Properties Manipulation**: Define and access complex data structures through headers
//!   and properties, enabling efficient data read and write.
//! - **Objective Reading**: Allows for selective reading of specific properties from a file, avoiding
//!   the need to process irrelevant data.
//! - **Dynamic Writing**: Supports adding new properties to existing files without compromising data
//!   integrity or performance.
//! - **Flexible Typing**: Supports a wide range of data types, from primitive types like integers and
//!   booleans to strings and floating-point types, making it easy to store diverse data.
//!
//! ## Use Cases
//!
//! - Efficient storage and retrieval of application settings or user data in binary formats.
//! - Implementation of custom file systems or specific file formats for games and applications requiring
//!   fast access to large volumes of data.
//! - Creation of serialization/deserialization tools that need fine control over data layout on disk.
//!
//! ## Basic Example
//!
//! ### Writing Data
//!
//! ```rust
//! use sheet::{BuilderHeader, BuilderProperties, DataType, Data};
//!
//! let mut builder_header = BuilderHeader::new();
//! builder_header.add("name".into(), DataType::Varchar(50)).unwrap();
//! builder_header.add("age".into(), DataType::U8).unwrap();
//! let header = builder_header.build();
//!
//! header.write("config_header.bin").unwrap();
//!
//! let mut builder_properties = BuilderProperties::new(&header);
//! builder_properties.add(Data::String("John Doe".into()));
//! builder_properties.add(Data::U8(30));
//! let properties = builder_properties.build();
//!
//! properties.write("config_properties.bin").unwrap();
//! ```
//!
//! ### Reading Data
//!
//! ```rust
//! use sheet::{Header, Properties};
//!
//! let mut header = Header::new();
//! header.read("config_header.bin").unwrap();
//!
//! let mut properties = Properties::new(&header);
//! properties.read("config_properties.bin").unwrap();
//!
//! let name = properties.get_by_label("name".as_bytes()).unwrap();
//! let age = properties.get_by_label("age".as_bytes()).unwrap();
//!
//! println!("Name: {}, Age: {}", name, age);
//! ```

use std::io;

mod macros;
mod header;
mod properties;
mod sort_key;

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

pub const DEFAULT_MAX_SIZE_PK_SK_NAMES: usize = 36; // UUID size

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
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
    ValueExists(String),
    SortNameSize,
    ParseString,
    SortKeyPosition,
}
