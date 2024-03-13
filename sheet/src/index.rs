//! # Index
//!
//! The index module is responsible for managing the index file.
//!
//! ## File schema
//! | total_size | sort_key_1 | sort_key_2 | ... | sort_key_n |
//! |------------|------------|------------|-----|------------|
//! | 4 bytes    | n bytes    | n bytes    | ... | n bytes    |
//!
use std::{fs::File, io::BufWriter};
use std::io::{BufReader, Seek, Write};
use std::io::Read;

use byteorder::ReadBytesExt;

use crate::{DEFAULT_LIMIT_INDEX_READ, DEFAULT_ORDER_INDEX_READ, DEFAULT_SIZE_U32, Error, INDEX_KEY_SIZE, SORT_KEY_SIZE, th_msg, UUID_SIZE};

pub fn write_index_raw(
    buffer_writer: &mut BufWriter<&File>,
    index: Vec<Vec<u8>>,
) -> Result<(), Error> {
    // Total size of the index
    th_msg!(
        buffer_writer.write_all(&(index.len() as u32).to_le_bytes()),
        Error::Io
    );

    for item in index {
        let mut buffer = vec![0; INDEX_KEY_SIZE];
        buffer[0..item.len()].copy_from_slice(&item);

        th_msg!(buffer_writer.write_all(&buffer), Error::Io);
    }

    Ok(())
}

pub fn read_index_raw(
    buffer_reader: &mut BufReader<&File>,
) -> Result<Vec<Vec<u8>>, Error> {
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index = Vec::with_capacity(total_size);

    for _ in 0..total_size {
        let mut buffer = vec![0; INDEX_KEY_SIZE];
        th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);
        index.push(buffer);
    }

    Ok(index)
}

pub fn write_index_ordered(
    buffer_writer: &mut BufWriter<&File>,
    index: Vec<Vec<u8>>,
) -> Result<(), Error> {
    let mut index = index;
    // sort asc index;
    index.sort();
    write_index_raw(buffer_writer, index)
}

pub fn add_item_index(
    buffer_writer: &mut BufWriter<&File>,
    buffer_reader: &mut BufReader<&File>,
    item: Vec<u8>,
) -> Result<(), Error> {
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index = {
        let mut buffer: Vec<u8> = vec![0; INDEX_KEY_SIZE];
        buffer[..item.len()].copy_from_slice(&item);

        let mut vec = Vec::with_capacity((total_size + 1) * INDEX_KEY_SIZE);
        vec.append(&mut buffer);
        vec
    };

    let mut find: bool = false;
    let mut position = total_size - 1;

    // TODO: no futuro, testar a performance com o rayon
    // talvez seja interessante paralelizar a busca caso tenha muitos itens
    for pos in 0..total_size {
        let mut buffer = vec![0; INDEX_KEY_SIZE];
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
            ((position * INDEX_KEY_SIZE) + DEFAULT_SIZE_U32) as u64
        )),
        Error::Io
    );

    th_msg!(buffer_writer.write_all(&index), Error::Io);

    Ok(())
}

// TODO: precisa refatorar para melhorar o desempenho da remoção, talvez usar rayon
// TODO: Isso é necessário pq a função esta varrendo todo o binario e jogano na memoria, caso o binario seja muito granda, vai dar ruim.
pub fn remove_item_index(
    buffer_writer: &mut BufWriter<&File>,
    buffer_reader: &mut BufReader<&File>,
    item: Vec<u8>,
) -> Result<(), Error> {
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index = Vec::with_capacity(total_size * INDEX_KEY_SIZE);

    let mut target: Vec<u8> = vec![0; INDEX_KEY_SIZE];
    target[..item.len()].copy_from_slice(&item);

    for _ in 0..total_size {
        let mut buffer = vec![0; INDEX_KEY_SIZE];
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
pub enum ReadIndexFilter<'a> {
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

#[derive(Debug, PartialEq)]
pub enum ReadIndexOrder {
    Asc,
    Desc,
}

#[derive(Debug)]
pub struct ReadIndexOptions<'a> {
    /// Filter to be used in the search
    pub filter: ReadIndexFilter<'a>,
    /// Limit of items to be returned
    pub limit: Option<usize>,
    /// Last position to start the search
    pub last_position: Option<usize>,
    /// Order of the search
    pub order: Option<ReadIndexOrder>,
}

impl Default for ReadIndexOptions<'_> {
    fn default() -> Self {
        ReadIndexOptions {
            filter: ReadIndexFilter::None,
            limit: None,
            last_position: None,
            order: None,
        }
    }
}

impl<'a> ReadIndexOptions<'a> {
    pub fn from_filter(filter: ReadIndexFilter<'a>) -> Self {
        Self {
            filter,
            limit: None,
            last_position: None,
            order: None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct IndexItem {
    pub item: Vec<u8>,
    pub hash: Vec<u8>,
    pub position: u32,
}

pub fn read_index_options(
    buffer_reader: &mut BufReader<&File>,
    options: ReadIndexOptions,
) -> Result<Vec<IndexItem>, Error> {
    let total_size = th_msg!(
        buffer_reader.read_u32::<byteorder::LittleEndian>(),
        Error::Io
    ) as usize;

    let mut index: Vec<IndexItem> = Vec::with_capacity(total_size);

    let limit = options.limit.unwrap_or(DEFAULT_LIMIT_INDEX_READ) as usize;

    let order = options.order.unwrap_or(DEFAULT_ORDER_INDEX_READ);

    let default_size_u32_as_u64 = DEFAULT_SIZE_U32 as u64;

    let last_position = options.last_position.unwrap_or(match order {
        ReadIndexOrder::Asc => 0,
        ReadIndexOrder::Desc => total_size,
    }) as usize;

    for pos in 0..total_size {
        if index.len() == limit {
            break;
        }

        let position = if order.eq(&ReadIndexOrder::Desc) {
            if last_position == pos {
                break;
            }

            let position = last_position - pos - 1;
            let byte_position = ((position * INDEX_KEY_SIZE) + DEFAULT_SIZE_U32) as u64;

            if byte_position < default_size_u32_as_u64 {
                break;
            }

            th_msg!(
                buffer_reader.seek(std::io::SeekFrom::Start(byte_position)),
                Error::Io
            );

            position as u32
        } else {
            let position = last_position + pos;
            let byte_position = ((position * INDEX_KEY_SIZE) + DEFAULT_SIZE_U32) as u64;

            if position >= total_size {
                break;
            }

            th_msg!(
                buffer_reader.seek(std::io::SeekFrom::Start(byte_position)),
                Error::Io
            );

            position as u32
        };

        let mut item = vec![0; SORT_KEY_SIZE];
        th_msg!(buffer_reader.read_exact(&mut item), Error::Io);

        let mut hash = vec![0; UUID_SIZE];
        th_msg!(buffer_reader.read_exact(&mut hash), Error::Io);

        match options.filter {
            ReadIndexFilter::StartWith(start) => {
                if item.starts_with(start) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                }
            }
            ReadIndexFilter::EndWith(end) => {
                if item.ends_with(end) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                } else {
                    let item_trim = item
                        .iter()
                        .filter(|&i| *i != 0)
                        .cloned()
                        .collect::<Vec<u8>>();

                    if item_trim.ends_with(&end) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }
                }
            }
            ReadIndexFilter::StartAndEndWith(start, end) => {
                if item.starts_with(start) && item.ends_with(end) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                } else {
                    let item_trim = item
                        .iter()
                        .filter(|&i| *i != 0)
                        .cloned()
                        .collect::<Vec<u8>>();

                    if item_trim.starts_with(&start) && item_trim.ends_with(&end) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }
                }
            }
            ReadIndexFilter::Contains(contains) => {
                for i in 0..item.len() {
                    if item[i..].starts_with(contains) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                        break;
                    }
                }
            }
            ReadIndexFilter::NotContains(not_contains) => {
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
                        position,
                    });
                }
            }
            ReadIndexFilter::Equal(equal) => {
                if item.len() > equal.len() {
                    let mut equal = equal.clone();
                    equal.resize(item.len(), 0);

                    if item.eq(&equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }

                    continue;
                }

                if item.eq(equal) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                }
            }
            ReadIndexFilter::NotEqual(not_equal) => {
                if item.len() > not_equal.len() {
                    let mut not_equal = not_equal.clone();
                    not_equal.resize(item.len(), 0);

                    if item.ne(&not_equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }

                    continue;
                }

                if item.ne(not_equal) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                }
            }
            ReadIndexFilter::GranterThan(granter_than) => {
                if item.len() > granter_than.len() {
                    let mut granter_than = granter_than.clone();
                    granter_than.resize(item.len(), 0);

                    if item.gt(&granter_than) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }

                    continue;
                }

                if item.gt(granter_than) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                }
            }
            ReadIndexFilter::LessThan(less_than) => {
                if item.len() > less_than.len() {
                    let mut less_than = less_than.clone();
                    less_than.resize(item.len(), 0);

                    if item.lt(&less_than) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }

                    continue;
                }

                if item.lt(less_than) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                }
            }
            ReadIndexFilter::GranterThanOrEqual(granter_than_or_equal) => {
                if item.len() > granter_than_or_equal.len() {
                    let mut granter_than_or_equal = granter_than_or_equal.clone();
                    granter_than_or_equal.resize(item.len(), 0);

                    if item.ge(&granter_than_or_equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }

                    continue;
                }

                if item.ge(granter_than_or_equal) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                }
            }
            ReadIndexFilter::LessThanOrEqual(less_than_or_equal) => {
                if item.len() > less_than_or_equal.len() {
                    let mut less_than_or_equal = less_than_or_equal.clone();
                    less_than_or_equal.resize(item.len(), 0);

                    if item.le(&less_than_or_equal) {
                        index.push(IndexItem {
                            item,
                            hash,
                            position,
                        });
                    }

                    continue;
                }

                if item.le(less_than_or_equal) {
                    index.push(IndexItem {
                        item,
                        hash,
                        position,
                    });
                }
            }
            ReadIndexFilter::None => {
                index.push(IndexItem {
                    item,
                    hash,
                    position,
                });
            }
        }
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use crate::{create_index_item, create_index_item_uuid, index_sort_key};

    use super::*;

    #[test]
    fn test_write_index_raw() {
        let file_name = "test_write_index_raw";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let index = vec![
            create_index_item!(b"aaaaa"),
            create_index_item!(b"bbbbb"),
            create_index_item!(b"ccccc"),
            create_index_item!(b"ddddd"),
        ];

        write_index_raw(&mut buffer_writer, index).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_raw() {
        let file_name = "test_read_index_raw";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let index = vec![
            create_index_item!(b"aaaaa"),
            create_index_item!(b"bbbbb"),
            create_index_item!(b"ccccc"),
            create_index_item!(b"ddddd"),
        ];

        write_index_raw(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();

        let mut buffer_reader = BufReader::new(&file);

        let index_read = read_index_raw(&mut buffer_reader).unwrap();

        assert_eq!(index, index_read);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_write_index_ordered() {
        let file_name = "test_write_index_ordered";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let index = vec![
            create_index_item!(b"ccccc"),
            create_index_item!(b"ddddd"),
            create_index_item!(b"aaaaa"),
            create_index_item!(b"bbbbb"),
        ];

        write_index_ordered(&mut buffer_writer, index).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_add_item_index() {
        let file_name = "test_add_item_index";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let index = vec![
            create_index_item!(b"1a"),
            create_index_item!(b"2b"),
            create_index_item!(b"3c"),
            create_index_item!(b"4e"),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::options()
            .write(true)
            .read(true)
            .open(file_name)
            .unwrap();
        let mut buffer_reader = BufReader::new(&file);
        let mut buffer_writer = BufWriter::new(&file);

        let item: Vec<u8> = create_index_item!(b"2c");

        add_item_index(
            &mut buffer_writer,
            &mut buffer_reader,
            item.clone(),
        )
        .unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();

        let mut buffer_reader = BufReader::new(&file);

        let index_read = {
            let data = read_index_raw(&mut buffer_reader).unwrap();
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

        let index = vec![
            create_index_item!(b"1a"),
            create_index_item!(b"2b"),
            create_index_item!(b"3c"),
            create_index_item!(b"4e"),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

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
        )
        .unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();

        let mut buffer_reader = BufReader::new(&file);

        let index_read = {
            let data = read_index_raw(&mut buffer_reader).unwrap();
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

        let index = vec![
            create_index_item!(b"hello world"),
            create_index_item!(b"hello dad"),
            create_index_item!(b"friend hello"),
            create_index_item!(b"code me"),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let contains = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::Contains(&b"hello".to_vec())),
        )
        .unwrap();
        assert_eq!(contains.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_contains = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::NotContains(&b"xxx".to_vec())),
        )
        .unwrap();
        assert_eq!(not_contains.len(), 4);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let start_with = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::StartWith(&b"hello".to_vec())),
        )
        .unwrap();
        assert_eq!(start_with.len(), 2);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let end_with = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::EndWith(&b"e".to_vec())),
        )
        .unwrap();
        assert_eq!(end_with.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let start_and_end_with = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::StartAndEndWith(
                &b"h".to_vec(),
                &b"dad".to_vec(),
            )),
        )
        .unwrap();
        assert_eq!(start_and_end_with.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_options_date() {
        let file_name = "test_read_index_options_date";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let index = vec![
            create_index_item!(b"2022-01-05 18:25:47"),
            create_index_item!(b"2022-05-05 18:25:48"),
            create_index_item!(b"2022-05-05 18:25:48"),
            create_index_item!(b"2022-05-05 18:25:49"),
            create_index_item!(b"2022-07-05 18:25:49"),
            create_index_item!(b"2022-09-05 18:25:50"),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::Equal(&b"2022-05-05 18:25:48".to_vec())),
        )
        .unwrap();
        assert_eq!(equal.len(), 2);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::NotEqual(
                &b"2022-05-05 18:25:48".to_vec(),
            )),
        )
        .unwrap();
        assert_eq!(not_equal.len(), 4);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::GranterThan(
                &b"2022-05-05 18:25:48".to_vec(),
            )),
        )
        .unwrap();
        assert_eq!(granter_than.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::LessThan(
                &b"2022-05-05 18:25:48".to_vec(),
            )),
        )
        .unwrap();
        assert_eq!(less_than.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than_or_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::GranterThanOrEqual(
                &b"2022-05-05 18:25:48".to_vec(),
            )),
        )
        .unwrap();
        assert_eq!(granter_than_or_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than_or_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::LessThanOrEqual(
                &b"2022-05-05 18:25:48".to_vec(),
            )),
        )
        .unwrap();
        assert_eq!(less_than_or_equal.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_options_math_alphabet() {
        let file_name = "test_read_index_options_math_alphabet";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let index = vec![
            create_index_item!(b"alice"),
            create_index_item!(b"bob"),
            create_index_item!(b"carlos"),
            create_index_item!(b"carol"),
            create_index_item!(b"david"),
            create_index_item!(b"edward"),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::Equal(&b"carol".to_vec())),
        )
        .unwrap();
        assert_eq!(equal.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::NotEqual(&b"carol".to_vec())),
        )
        .unwrap();
        assert_eq!(not_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::GranterThan(&b"carol".to_vec())),
        )
        .unwrap();
        assert_eq!(granter_than.len(), 2);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::LessThan(&b"carol".to_vec())),
        )
        .unwrap();
        assert_eq!(less_than.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than_or_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::GranterThanOrEqual(&b"carol".to_vec())),
        )
        .unwrap();
        assert_eq!(granter_than_or_equal.len(), 3);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than_or_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::LessThanOrEqual(&b"carol".to_vec())),
        )
        .unwrap();
        assert_eq!(less_than_or_equal.len(), 4);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_options_math_number() {
        let file_name = "test_read_index_options_math_number";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let index = vec![
            create_index_item!(b"100"),
            create_index_item!(b"200"),
            create_index_item!(b"200.1"),
            create_index_item!(b"300.2"),
            create_index_item!(b"300.1"),
            create_index_item!(b"300"),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::Equal(&b"200".to_vec())),
        )
        .unwrap();
        assert_eq!(equal.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let not_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::NotEqual(&b"200".to_vec())),
        )
        .unwrap();
        assert_eq!(not_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::GranterThan(&b"200".to_vec())),
        )
        .unwrap();
        assert_eq!(granter_than.len(), 4);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::LessThan(&b"200".to_vec())),
        )
        .unwrap();
        assert_eq!(less_than.len(), 1);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let granter_than_or_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::GranterThanOrEqual(&b"200".to_vec())),
        )
        .unwrap();
        assert_eq!(granter_than_or_equal.len(), 5);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let less_than_or_equal = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions::from_filter(ReadIndexFilter::LessThanOrEqual(&b"200".to_vec())),
        )
        .unwrap();
        assert_eq!(less_than_or_equal.len(), 2);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_order() {
        let file_name = "test_read_index_order";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let item1 = create_index_item_uuid!(b"aaaaa");
        let item2 = create_index_item_uuid!(b"bbbbb");
        let item3 = create_index_item_uuid!(b"ccccc");
        let item4 = create_index_item_uuid!(b"ddddd");

        let index = vec![
            item1.0.clone(),
            item2.0.clone(),
            item3.0.clone(),
            item4.0.clone(),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let asc = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions {
                filter: ReadIndexFilter::None,
                limit: None,
                last_position: None,
                order: Some(ReadIndexOrder::Asc),
            },
        )
        .unwrap();
        assert_eq!(asc.len(), 4);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let desc = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions {
                filter: ReadIndexFilter::None,
                limit: Some(2),
                last_position: None,
                order: Some(ReadIndexOrder::Desc),
            },
        )
        .unwrap();

        assert_eq!(vec![
            IndexItem {
                // this string need to be 100 bytes
                item: index_sort_key!(b"ddddd"),
                hash: item4.1,
                position: 3
            },
            IndexItem {
                item: index_sort_key!(b"ccccc"),
                hash: item3.1,
                position: 2
            }
        ], desc);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let asc = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions {
                filter: ReadIndexFilter::None,
                limit: Some(2),
                last_position: None,
                order: Some(ReadIndexOrder::Asc),
            },
        )
        .unwrap();

        assert_eq!(vec![
            IndexItem {
                item: index_sort_key!(b"aaaaa"),
                hash: item1.1,
                position: 0
            },
            IndexItem {
                item: index_sort_key!(b"bbbbb"),
                hash: item2.1,
                position: 1
            },
        ], asc);

        remove_file(file_name).unwrap();
    }

    #[test]
    fn test_read_index_last_position_and_order() {
        let file_name = "test_read_index_last_position_and_order";
        let file = File::create(file_name).unwrap();
        let mut buffer_writer = BufWriter::new(&file);

        let item1 = create_index_item_uuid!(b"aaaaa");
        let item2 = create_index_item_uuid!(b"bbbbb");
        let item3 = create_index_item_uuid!(b"ccccc");
        let item4 = create_index_item_uuid!(b"ddddd");

        let index = vec![
            item1.0.clone(),
            item2.0.clone(),
            item3.0.clone(),
            item4.0.clone(),
        ];

        write_index_ordered(&mut buffer_writer, index.clone()).unwrap();

        buffer_writer.flush().unwrap();

        let file = File::open(file_name).unwrap();
        let mut buffer_reader = BufReader::new(&file);

        let asc = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions {
                filter: ReadIndexFilter::None,
                limit: None,
                last_position: Some(2),
                order: Some(ReadIndexOrder::Asc),
            },
        )
        .unwrap();
        assert_eq!(asc.len(), 2);

        buffer_reader.seek(std::io::SeekFrom::Start(0)).unwrap();

        let desc = read_index_options(
            &mut buffer_reader,
            ReadIndexOptions {
                filter: ReadIndexFilter::None,
                limit: None,
                last_position: Some(2),
                order: Some(ReadIndexOrder::Desc),
            },
        )
        .unwrap();

        assert_eq!(vec![
            IndexItem {
                item: index_sort_key!(b"bbbbb"),
                hash: item2.1,
                position: 1
            },
            IndexItem {
                item: index_sort_key!(b"aaaaa"),
                hash: item1.1,
                position: 0
            }
        ], desc);

        remove_file(file_name).unwrap();
    }
}
