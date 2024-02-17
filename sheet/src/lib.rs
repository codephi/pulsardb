//! # Sheet
//! 
//! Sheet is a library to read and write data to a binary file.
//! # Features
//! - Read and write header to a binary file
//! - Read and write properties to a binary file
//! - Read full binary properties file.
//! - Provides objective reading, capturing only the necessary properties.
//! 
//! # Example
//! ```
//! use sheet::{BuilderHeader, BuildProperties};
//! 
//! let mut header = BuilderHeader::new()
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