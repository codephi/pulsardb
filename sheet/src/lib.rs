use std::io;

pub mod header;
pub(crate) mod macros;
pub mod properties;

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

pub const NULL_BIN_VALUE: u8 = 0;
pub const FALSE_BIN_VALUE: u8 = 0;
pub const TRUE_BIN_VALUE: u8 = 1;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    WriteInvalidDataType,
    ReadInvalidDataType,
    VarcharSize,
    NumberParse,
}
