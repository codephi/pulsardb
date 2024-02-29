// resolve!(File::create("file.bin"), Error::Io);
// compile:
// match File::create("file.bin") {
//     Ok(file) => file,
//     Err(e) => return Err(Error::Io(e)),
// }
/// Macro to handle Result and return a custom error
/// # Example
/// ```
/// let file = th_err!(File::create("file.bin"), Error::Io);
/// ```
/// # Expands to
/// ```
/// match File::create("file.bin") {
///    Ok(file) => file,
///   Err(e) => return Err(Error::Io(e)),
/// }
/// ```
#[macro_export]
macro_rules! th_msg {
    ($result:expr, $error:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => return Err($error(e)),
        }
    };
}

#[macro_export]
macro_rules! th {
    ($result:expr, $error:expr) => {
        match $result {
            Ok(value) => value,
            Err(_) => return Err($error),
        }
    };
}

#[macro_export]
macro_rules! th_none {
    ($result:expr, $error:expr) => {
        match $result {
            Some(value) => value,
            None => return Err($error),
        }
    };
}

#[macro_export]
macro_rules! uuid {
    () => {
        uuid::Uuid::new_v4().as_simple().to_string().as_bytes().to_vec() 
    };
}

#[macro_export]
macro_rules! uuid_string {
    () => {
        uuid::Uuid::new_v4().as_simple().to_string()
    };
}

#[macro_export]
macro_rules! index_item {
    ($uuid:expr, $prop:expr, $total_size:expr) => {
        {
            let mut item = vec![0u8; $total_size];
            item[..UUID_SIZE].copy_from_slice(&$uuid);
            item[UUID_SIZE..(UUID_SIZE + $prop.len())].copy_from_slice($prop);

            item
        }
    };
}

#[macro_export]
macro_rules! create_index_item {
    ($prop:expr, $total_size:expr) => {
        {
            let uuid = crate::uuid!();
            crate::index_item!(uuid, $prop, $total_size)
        }
    };
}