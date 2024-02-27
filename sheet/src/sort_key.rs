// pub fn write_sort_key(
//     buffer_writer: &mut BufWriter<File>,
//     headers: std::slice::Iter<'_, PropertyHeader>,
// ) -> Result<(), Error> {
//     // Write total size
//     th_msg!(
//         buffer_writer.write_all(&(headers.len() as u32).to_le_bytes()),
//         Error::Io
//     );

//     for prop in headers {
//         let data_type_byte = prop.data_type.clone().into();

//         // Write original position
//         th_msg!(
//             buffer_writer.write_all(&(prop.original_position as u32).to_le_bytes()),
//             Error::Io
//         );

//         // Write data type
//         th_msg!(buffer_writer.write_all(&[data_type_byte]), Error::Io);

//         if let DataType::Varchar(size) = prop.data_type {
//             th_msg!(buffer_writer.write_all(&size.to_le_bytes()), Error::Io);
//         }

//         // Write label size
//         th_msg!(
//             buffer_writer.write_all(&(prop.label.len() as u32).to_le_bytes()),
//             Error::Io
//         );
//         // Write label
//         th_msg!(buffer_writer.write_all(&prop.label), Error::Io);
//     }

//     Ok(())
// }