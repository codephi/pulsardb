use crate::{th_msg, Error, DEFAULT_SIZE_U32, UUID_SIZE};
use byteorder::ReadBytesExt;
use std::io::Read;
use std::io::{BufReader, Seek, Write};
use std::{fs::File, io::BufWriter};

pub fn write_index_raw(
    buffer_writer: &mut BufWriter<&File>,
    index: Vec<Vec<u8>>,
    size_index_item: u8,
) -> Result<(), Error> {
    // Total size of the index
    th_msg!(
        buffer_writer.write_all(&(index.len() as u32).to_le_bytes()),
        Error::Io
    );

    for item in index {
        let mut buffer = vec![0u8; size_index_item as usize];
        buffer[..item.len()].copy_from_slice(&item);

        th_msg!(buffer_writer.write_all(&buffer), Error::Io);
    }

    Ok(())
}

pub fn read_index_raw(
    buffer_reader: &mut BufReader<&File>,
    size_index_item: u8,
) -> Result<Vec<Vec<u8>>, Error> {
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index = Vec::with_capacity(total_size);

    for _ in 0..total_size {
        let mut buffer = vec![0u8; size_index_item as usize];
        th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);
        index.push(buffer);
    }

    Ok(index)
}

pub fn write_index_ordered(
    buffer_writer: &mut BufWriter<&File>,
    index: Vec<Vec<u8>>,
    size_index_item: u8,
) -> Result<(), Error> {
    let mut index = index;
    // sort asc index;
    index.sort();
    write_index_raw(buffer_writer, index, size_index_item)
}

pub fn add_item_index(
    buffer_writer: &mut BufWriter<&File>,
    buffer_reader: &mut BufReader<&File>,
    item: Vec<u8>,
    size_index_item: u8,
) -> Result<(), Error> {
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index = {
        let mut buffer: Vec<u8> = vec![0u8; size_index_item as usize];
        buffer[..item.len()].copy_from_slice(&item);

        let mut vec = Vec::with_capacity((total_size + 1) * size_index_item as usize);
        vec.append(&mut buffer);
        vec
    };

    let mut find: bool = false;
    let mut position = total_size - 1;

    // TODO: no futuro, testar a performance com o rayon
    // talvez seja interessante paralelizar a busca caso tenha muitos itens
    for pos in 0..total_size {
        let mut buffer = vec![0u8; size_index_item as usize];
        th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

        if item < buffer {
            if !find {
                position = pos;
                find = true;
            }

            index.append(&mut buffer);
        }
    }

    th_msg!(buffer_writer.seek(std::io::SeekFrom::Start(0)), Error::Io);

    th_msg!(
        buffer_writer.write_all(&(total_size as u32 + 1).to_le_bytes()),
        Error::Io
    );

    th_msg!(
        buffer_writer.seek(std::io::SeekFrom::Start(
            (position as u64 * size_index_item as u64) + DEFAULT_SIZE_U32 as u64
        )),
        Error::Io
    );

    th_msg!(buffer_writer.write_all(&index), Error::Io);

    Ok(())
}

// TODO: precisa refatorar para melhorar o desempenho da remoção
pub fn remove_item_index(
    buffer_writer: &mut BufWriter<&File>,
    buffer_reader: &mut BufReader<&File>,
    item: Vec<u8>,
    size_index_item: u8,
) -> Result<(), Error> {
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index = Vec::with_capacity(total_size * size_index_item as usize);

    let mut position = total_size - 1;

    let mut target: Vec<u8> = vec![0u8; size_index_item as usize];
    target[..item.len()].copy_from_slice(&item);

    for pos in 0..total_size {
        let mut buffer = vec![0u8; size_index_item as usize];
        th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

        if target == buffer {
            position = pos;
            continue;
        }

        index.append(&mut buffer);
    }

    th_msg!(buffer_writer.seek(std::io::SeekFrom::Start(0)), Error::Io);

    th_msg!(
        buffer_writer.write_all(&(total_size as u32 - 1).to_le_bytes()),
        Error::Io
    );

    th_msg!(
        buffer_writer.seek(std::io::SeekFrom::Start(DEFAULT_SIZE_U32 as u64)),
        Error::Io
    );

    th_msg!(buffer_writer.write_all(&index), Error::Io);

    Ok(())
}

#[derive(Debug)]
pub enum ReadIndexOption {
    StartWith(&'static Vec<u8>),
    EndWith(&'static Vec<u8>),
    StartAndEndWith(&'static Vec<u8>, &'static Vec<u8>),
    Contains(&'static Vec<u8>),
    NotContains(&'static Vec<u8>),
    Equal(&'static Vec<u8>),
    NotEqual(&'static Vec<u8>),
    GranterThan(&'static Vec<u8>),
    LessThan(&'static Vec<u8>),
    GranterThanOrEqual(&'static Vec<u8>),
    LessThanOrEqual(&'static Vec<u8>),
    None,
}

#[derive(Debug)]
pub struct IndexItem {
    pub item: Vec<u8>,
    pub hash: Vec<u8>,
    pub position: u32,
}

pub fn read_index_options(
    buffer_reader: &mut BufReader<&File>,
    size_index_item: u8,
    option: ReadIndexOption,
) -> Result<Vec<IndexItem>, Error> {
    let size_index_item_u32 = size_index_item as u32;
    let size_item: usize = size_index_item as usize - UUID_SIZE;
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index: Vec<IndexItem> = Vec::with_capacity(total_size);

    for pos in 0..total_size {
        let mut item = vec![0u8; size_item];
        th_msg!(buffer_reader.read_exact(&mut item), Error::Io);

        let mut hash = vec![0u8; UUID_SIZE];
        th_msg!(buffer_reader.read_exact(&mut hash), Error::Io);

        match option {
            ReadIndexOption::StartWith(start) => {
                if item.starts_with(start) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::EndWith(end) => {
                if item.ends_with(end) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::StartAndEndWith(start, end) => {
                if item.starts_with(start) && item.ends_with(end) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::Contains(contains) => {
                if item.contains(contains) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::NotContains(not_contains) => {
                if !item.contains(not_contains) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::Equal(equal) => {
                if item == equal {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::NotEqual(not_equal) => {
                //compare if is not equal
                if item != not_equal {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::GranterThan(granter_than) => {
                //compare if is granter than
                if item > granter_than {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::LessThan(less_than) => {
                //compare if is less than
                if item < less_than {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::GranterThanOrEqual(granter_than_or_equal) => {
                //compare if is granter than or equal
                if item >= granter_than_or_equal {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::LessThanOrEqual(less_than_or_equal) => {
                //compare if is less than or equal
                if item <= less_than_or_equal {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::None => {
                index.push(IndexItem {
                    item,
                    hash,
                    position: pos as u32,
                });
            }
        }
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{create_index_item, UUID_SIZE};
    use std::fs::remove_file;

    #[test]
    fn test_write_index_raw() {
        let file_name = "test_write_index_raw";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 5;

        let index = vec![
            create_index_item!(b"aaaaa", size_index_item),
            create_index_item!(b"bbbbb", size_index_item),
            create_index_item!(b"ccccc", size_index_item),
            create_index_item!(b"ddddd", size_index_item),
        ];

        write_index_raw(&mut buffer_writer, index, size_index_item as u8).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_raw() {
        let file_name = "test_read_index_raw";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 5;

        let index = vec![
            create_index_item!(b"aaaaa", size_index_item),
            create_index_item!(b"bbbbb", size_index_item),
            create_index_item!(b"ccccc", size_index_item),
            create_index_item!(b"ddddd", size_index_item),
        ];

        write_index_raw(&mut buffer_writer, index.clone(), size_index_item as u8).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();

        let mut buffer_reader = BufReader::new(&file);

        let index_read = read_index_raw(&mut buffer_reader, size_index_item as u8).unwrap();

        assert_eq!(index, index_read);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_write_index_ordered() {
        let file_name = "test_write_index_ordered";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 5;

        let index = vec![
            create_index_item!(b"ccccc", size_index_item),
            create_index_item!(b"ddddd", size_index_item),
            create_index_item!(b"aaaaa", size_index_item),
            create_index_item!(b"bbbbb", size_index_item),
        ];

        write_index_ordered(&mut buffer_writer, index, size_index_item as u8).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_add_item_index() {
        let file_name = "test_add_item_index";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 5;

        let index = vec![
            create_index_item!(b"1a", size_index_item),
            create_index_item!(b"2b", size_index_item),
            create_index_item!(b"3c", size_index_item),
            create_index_item!(b"4e", size_index_item),
        ];

        write_index_ordered(&mut buffer_writer, index.clone(), size_index_item as u8).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::options()
            .write(true)
            .read(true)
            .open(file_name)
            .unwrap();
        let mut buffer_reader = BufReader::new(&file);
        let mut buffer_writer = BufWriter::new(&file);

        let item: Vec<u8> = create_index_item!(b"2c", size_index_item);

        add_item_index(
            &mut buffer_writer,
            &mut buffer_reader,
            item.clone(),
            size_index_item as u8,
        )
        .unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();

        let mut buffer_reader = BufReader::new(&file);

        let index_read = {
            let data = read_index_raw(&mut buffer_reader, size_index_item as u8).unwrap();
            data.iter()
                .map(|item| String::from_utf8(item.clone()).unwrap())
                .collect::<Vec<String>>()
        };

        let compare = {
            let mut index = index.clone();
            // Insert the item in the correct position
            index.insert(2, item);
            index
                .iter()
                .map(|item| String::from_utf8(item.clone()).unwrap())
                .collect::<Vec<String>>()
        };

        assert_eq!(compare, index_read);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_remove_item_index() {
        let file_name = "test_remove_item_index";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 5;

        let index = vec![
            create_index_item!(b"1a", size_index_item),
            create_index_item!(b"2b", size_index_item),
            create_index_item!(b"3c", size_index_item),
            create_index_item!(b"4e", size_index_item),
        ];

        write_index_ordered(&mut buffer_writer, index.clone(), size_index_item as u8).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::options()
            .write(true)
            .read(true)
            .open(file_name)
            .unwrap();
        let mut buffer_reader = BufReader::new(&file);
        let mut buffer_writer = BufWriter::new(&file);

        remove_item_index(
            &mut buffer_writer,
            &mut buffer_reader,
            index.get(1).unwrap().clone(),
            size_index_item as u8,
        )
        .unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();

        let mut buffer_reader = BufReader::new(&file);

        let index_read = {
            let data = read_index_raw(&mut buffer_reader, size_index_item as u8).unwrap();
            data.iter()
                .map(|item| String::from_utf8(item.clone()).unwrap())
                .collect::<Vec<String>>()
        };

        let compare = {
            let mut index = index.clone();
            index.remove(1);
            index
                .iter()
                .map(|item| String::from_utf8(item.clone()).unwrap())
                .collect::<Vec<String>>()
        };

        assert_eq!(compare, index_read);

        remove_file(file_name).unwrap();
    }
}
