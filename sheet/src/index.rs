use crate::{index, th_msg, Error, DEFAULT_SIZE_U32};
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

    let max_position = total_size - 1;
    let mut position = max_position;
    let mut total_items_after = max_position - position;
    let mut index = vec![item.clone()];

    for pos in 0..total_size {
        let mut buffer = vec![0u8; size_index_item as usize];
        th_msg!(buffer_reader.read_exact(&mut buffer), Error::Io);

        if item < buffer {
            if index.len() == 1 {
                position = pos;
            }

            index.push(buffer);
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

    for item in index {
        let mut buffer = vec![0u8; size_index_item as usize];
        buffer[..item.len()].copy_from_slice(&item);
        th_msg!(buffer_writer.write_all(&buffer), Error::Io);
    }

    Ok(())
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

        assert_eq!(index_read, compare);

        remove_file(file_name).unwrap();
    }
}
