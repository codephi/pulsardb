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
}

/*
 let json_example = json!({
    "profile": {
        "name": "John Doe",
        "age": 30,
        "height": 1.80,
        "is_student": false,
        "address": {
            "city": "New York",
            "state": "NY",
            "country": "USA"
        }
    },
    "products": [
        {
            "name": "product 1",
            "price": 10.0,
            "quantity": 2,
            "description": "description",
            "images": [
                {
                    "url": "http://example.com/image1.jpg",
                    "description": "image 1"
                },
                {
                    "url": "http://example.com/image2.jpg",
                    "description": "image 2"
                }
            ]
        },
        {
            "name": "product 2",
            "price": 20.0,
            "quantity": 1
        }
    ],
 })

    let header = vec![
        PropertyHeader::from(("profile$name", DataType::Varchar(30))),
        PropertyHeader::from(("profile$age", DataType::U8)),
        PropertyHeader::from(("profile$height", DataType::F64)),
        PropertyHeader::from(("profile$is_student", DataType::Boolean)),
        PropertyHeader::from(("profile$address$city", DataType::Varchar(30))),
        PropertyHeader::from(("profile$address$state", DataType::Varchar(30))),
        PropertyHeader::from(("profile$address$country", DataType::Varchar(30))),
        PropertyHeader::from(("products$0$name", DataType::Varchar(30))),
        PropertyHeader::from(("products$0$price", DataType::F64)),
        PropertyHeader::from(("products$0$quantity", DataType::U8)),
        PropertyHeader::from(("products$0$images$0$url", DataType::Varchar(30))),
        PropertyHeader::from(("products$0$images$0$description", DataType::Varchar(30))),
        PropertyHeader::from(("products$0$images$1$url", DataType::Varchar(30))),
        PropertyHeader::from(("products$0$images$1$description", DataType::Varchar(30))),
        PropertyHeader::from(("products$1$name", DataType::Varchar(30))),
        PropertyHeader::from(("products$1$price", DataType::F64)),
        PropertyHeader::from(("products$1$quantity", DataType::U8)),
    ];

    let typed_json = json!({
        {
            "profile": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string"
                    },
                    "age": {
                        "type": "number"
                    },
                    "height": {
                        "type": "number"
                    },
                    "is_student": {
                        "type": "boolean"
                    },
                    "address": {
                        "type": "object",
                        "properties": {
                            "city": {
                                "type": "string",
                                "length": 30
                            },
                            "state": {
                                "type": "string",
                                "length": 30
                            },
                            "country": {
                                "type": "string",
                                "length": 30
                            }
                        }
                    }
                }
            },
            "products": {
                "type": "array",
                "length": 10,
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                            "length": 30
                        },
                        "price": {
                            "type": "number",
                            "length": "f32"
                        },
                        "quantity": {
                            "type": "number",
                            "length": "u8"
                        },
                        "description": {
                            "type": "string"
                        },
                        "images": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "url": {
                                        "type": "string"
                                    },
                                    "description": {
                                        "type": "string"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
*/
