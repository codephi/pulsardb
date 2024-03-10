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
        let mut buffer = vec![0; size_index_item as usize];
        buffer[0..item.len()].copy_from_slice(&item);

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
        let mut buffer = vec![0; size_index_item as usize];
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
        let mut buffer: Vec<u8> = vec![0; size_index_item as usize];
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
        let mut buffer = vec![0; size_index_item as usize];
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

    let mut target: Vec<u8> = vec![0; size_index_item as usize];
    target[..item.len()].copy_from_slice(&item);

    for _ in 0..total_size {
        let mut buffer = vec![0; size_index_item as usize];
        th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

        if target == buffer {
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
pub enum ReadIndexOption<'a> {
    StartWith(&'a Vec<u8>),
    EndWith(&'a Vec<u8>),
    StartAndEndWith(&'a Vec<u8>, &'a Vec<u8>),
    Contains(&'a Vec<u8>),
    NotContains(&'a Vec<u8>),
    Equal(&'a Vec<u8>),
    NotEqual(&'a Vec<u8>),
    GranterThan(&'a Vec<u8>),
    LessThan(&'a Vec<u8>),
    GranterThanOrEqual(&'a Vec<u8>),
    LessThanOrEqual(&'a Vec<u8>),
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
    let size_item: usize = size_index_item as usize - UUID_SIZE;
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index: Vec<IndexItem> = Vec::with_capacity(total_size);

    for pos in 0..total_size {
        let mut item = vec![0; size_item];
        th_msg!(buffer_reader.read_exact(&mut item), Error::Io);

        let mut hash = vec![0; UUID_SIZE];
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
                } else {
                    let item_trim = item.iter().filter(|&i| *i != 0).cloned().collect::<Vec<u8>>();

                    if item_trim.ends_with(&end) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }
                }
            }
            ReadIndexOption::StartAndEndWith(start, end) => {
                if item.starts_with(start) && item.ends_with(end) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                } else {
                    let item_trim = item.iter().filter(|&i| *i != 0).cloned().collect::<Vec<u8>>();

                    if item_trim.starts_with(&start) && item_trim.ends_with(&end) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }
                }
            }
            ReadIndexOption::Contains(contains) => {
                for i in 0..item.len() {
                    if item[i..].starts_with(contains) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                        break;
                    }
                }
            }
            ReadIndexOption::NotContains(not_contains) => {
                let mut found = false;
                for i in 0..item.len() {
                    if item[i..].starts_with(not_contains) {
                        found = true;
                        break;
                    }
                }

                if !found {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::Equal( equal) => {
                if item.len() > equal.len() {
                    let mut equal = equal.clone();
                    equal.resize(item.len(), 0);

                    if item.eq(&equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }

                    continue;
                }

                if item.eq(equal) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::NotEqual(not_equal) => {
                if item.len() > not_equal.len() {
                    let mut not_equal = not_equal.clone();
                    not_equal.resize(item.len(), 0);

                    if item.ne(&not_equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }

                    continue;
                }

                if item.ne(not_equal) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::GranterThan(granter_than) => {
                if item.len() > granter_than.len() {
                    let mut granter_than = granter_than.clone();
                    granter_than.resize(item.len(), 0);

                    if item.gt(&granter_than) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }

                    continue;
                }

                if item.gt(granter_than) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::LessThan(less_than) => {
                if item.len() > less_than.len() {
                    let mut less_than = less_than.clone();
                    less_than.resize(item.len(), 0);

                    if item.lt(&less_than) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }

                    continue;
                }

                if item.lt(less_than) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::GranterThanOrEqual(granter_than_or_equal) => {
                if item.len() > granter_than_or_equal.len() {
                    let mut granter_than_or_equal = granter_than_or_equal.clone();
                    granter_than_or_equal.resize(item.len(), 0);

                    if item.ge(&granter_than_or_equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }

                    continue;
                }

                if item.ge(granter_than_or_equal) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position: pos as u32,
                    });
                }
            }
            ReadIndexOption::LessThanOrEqual(less_than_or_equal) => {
                if item.len() > less_than_or_equal.len() {
                    let mut less_than_or_equal = less_than_or_equal.clone();
                    less_than_or_equal.resize(item.len(), 0);

                    if item.le(&less_than_or_equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position: pos as u32,
                        });
                    }

                    continue;
                }

                if item.le(less_than_or_equal) {
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

    #[test]
    fn test_read_index_options() {
        let file_name = "test_read_index_options";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 15;

        let index = vec![
            create_index_item!(b"hello world", size_index_item),
            create_index_item!(b"hello dad", size_index_item),
            create_index_item!(b"friend hello", size_index_item),
            create_index_item!(b"code me", size_index_item),
        ];

        write_index_ordered(&mut buffer_writer, index.clone(), size_index_item as u8).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let contains = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::Contains(&b"hello".to_vec())).unwrap();
        assert_eq!(contains.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_contains = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::NotContains(&b"xxx".to_vec())).unwrap();
        assert_eq!(not_contains.len(), 4);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let start_with = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::StartWith(&b"hello".to_vec())).unwrap();
        assert_eq!(start_with.len(), 2);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let end_with = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::EndWith(&b"e".to_vec())).unwrap();
        assert_eq!(end_with.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let start_and_end_with = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::StartAndEndWith(&b"h".to_vec(), &b"dad".to_vec())).unwrap();
        assert_eq!(start_and_end_with.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_options_date() {
        let file_name = "test_read_index_options_date";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 20;

        let index = vec![
            create_index_item!(b"2022-01-05 18:25:47", size_index_item),
            create_index_item!(b"2022-05-05 18:25:48", size_index_item),
            create_index_item!(b"2022-05-05 18:25:48", size_index_item),
            create_index_item!(b"2022-05-05 18:25:49", size_index_item),
            create_index_item!(b"2022-07-05 18:25:49", size_index_item),
            create_index_item!(b"2022-09-05 18:25:50", size_index_item),
        ];

        write_index_ordered(&mut buffer_writer, index.clone(), size_index_item as u8).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::Equal(&b"2022-05-05 18:25:48".to_vec())).unwrap();
        assert_eq!(equal.len(), 2);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::NotEqual(&b"2022-05-05 18:25:48".to_vec())).unwrap();
        assert_eq!(not_equal.len(), 4);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::GranterThan(&b"2022-05-05 18:25:48".to_vec())).unwrap();
        assert_eq!(granter_than.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::LessThan(&b"2022-05-05 18:25:48".to_vec())).unwrap();
        assert_eq!(less_than.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than_or_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::GranterThanOrEqual(&b"2022-05-05 18:25:48".to_vec())).unwrap();
        assert_eq!(granter_than_or_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than_or_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::LessThanOrEqual(&b"2022-05-05 18:25:48".to_vec())).unwrap();
        assert_eq!(less_than_or_equal.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_options_math_alphabet() {
        let file_name = "test_read_index_options_math_alphabet";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 10;

        let index = vec![
            create_index_item!(b"alice", size_index_item),
            create_index_item!(b"bob", size_index_item),
            create_index_item!(b"carlos", size_index_item),
            create_index_item!(b"carol", size_index_item),
            create_index_item!(b"david", size_index_item),
            create_index_item!(b"edward", size_index_item),
        ];

        write_index_ordered(&mut buffer_writer, index.clone(), size_index_item as u8).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::Equal(&b"carol".to_vec())).unwrap();
        assert_eq!(equal.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::NotEqual(&b"carol".to_vec())).unwrap();
        assert_eq!(not_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::GranterThan(&b"carol".to_vec())).unwrap();
        assert_eq!(granter_than.len(), 2);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::LessThan(&b"carol".to_vec())).unwrap();
        assert_eq!(less_than.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than_or_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::GranterThanOrEqual(&b"carol".to_vec())).unwrap();
        assert_eq!(granter_than_or_equal.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than_or_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::LessThanOrEqual(&b"carol".to_vec())).unwrap();
        assert_eq!(less_than_or_equal.len(), 4);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_options_math_number() {
        let file_name = "test_read_index_options_math_alphabet";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);
        let size_index_item = UUID_SIZE + 10;

        let index = vec![
            create_index_item!(b"100", size_index_item),
            create_index_item!(b"200", size_index_item),
            create_index_item!(b"200.1", size_index_item),
            create_index_item!(b"300.2", size_index_item),
            create_index_item!(b"300.1", size_index_item),
            create_index_item!(b"300", size_index_item),
        ];

        write_index_ordered(&mut buffer_writer, index.clone(), size_index_item as u8).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::Equal(&b"200".to_vec())).unwrap();
        assert_eq!(equal.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::NotEqual(&b"200".to_vec())).unwrap();
        assert_eq!(not_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::GranterThan(&b"200".to_vec())).unwrap();
        assert_eq!(granter_than.len(), 4);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::LessThan(&b"200".to_vec())).unwrap();
        assert_eq!(less_than.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than_or_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::GranterThanOrEqual(&b"200".to_vec())).unwrap();
        assert_eq!(granter_than_or_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than_or_equal = read_index_options(&mut buffer_reader, size_index_item as u8, ReadIndexOption::LessThanOrEqual(&b"200".to_vec())).unwrap();
        assert_eq!(less_than_or_equal.len(), 2);

        remove_file(file_name).unwrap();
    }
}
