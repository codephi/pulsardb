# Sheet

Sheet is a Rust library designed for efficient and safe reading and writing of data in binary files. With a focus on simplicity and performance, it enables developers to handle complex data structures persisted on disk, optimizing both read and write operations.

## License

This project is licensed under either of:

- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

at your option.

## Key Features

- Define and access complex data structures through headers and properties.
- Selective reading of specific properties from a file.
- Support for adding new properties to existing files without compromising data integrity or performance.
- Wide range of supported data types, from primitive types to strings and floating points.

## Use Cases

Sheet can be particularly useful for:
- Efficient storage and retrieval of application settings or user data in binary formats.
- Implementing custom file systems or specific file formats for applications requiring fast access to large volumes of data, such as games.
- Creating serialization/deserialization tools with fine control over the data layout on disk.

## Installation

Add Sheet to your `Cargo.toml`:

```toml
[dependencies]
sheet = "0.1.0"
```

## Getting Started
Here's a simple example of how to use Sheet to write and read data:

### Writing Data
```rust
use sheet::{BuilderHeader, BuilderProperties, DataType, Data};

let mut builder_header = BuilderHeader::new();
builder_header.add("name".into(), DataType::Varchar(50)).unwrap();
builder_header.add("age".into(), DataType::U8).unwrap();
let header = builder_header.build();

header.write("config_header.bin").unwrap();

let mut builder_properties = BuilderProperties::new(&header);
builder_properties.add(Data::String("John Doe".into()));
builder_properties.add(Data::U8(30));
let properties = builder_properties.build();

properties.write("config_properties.bin").unwrap();
```

### Reading Data
```rust
use sheet::{Header, Properties};

let mut header = Header::new();
header.read("config_header.bin").unwrap();

let mut properties = Properties::new(&header);
properties.read("config_properties.bin").unwrap();

let name = properties.get_by_label("name".as_bytes()).unwrap();
let age = properties.get_by_label("age".as_bytes()).unwrap();

println!("Name: {}, Age: {}", name, age);
```

## Contributing
Contributions are welcome! Please feel free to submit pull requests or open issues to improve the library or documentation.

## Contact
For questions or suggestions regarding Sheet, please open an issue on GitHub.

