// resolve!(File::create("file.bin"), Error::Io);
// compile:
// match File::create("file.bin") {
//     Ok(file) => file,
//     Err(e) => return Err(Error::Io(e)),
// }
/// Macro to handle Result and return a custom error
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
macro_rules! index_sort_key {
    ($prop:expr) => {
        {
            let mut item = vec![0; crate::SORT_KEY_SIZE];
            item[..$prop.len()].copy_from_slice($prop);
            item
        }
    };
}


#[macro_export]
macro_rules! index_item {
    ($uuid:expr, $prop:expr) => {
        {
            let mut item = vec![0; crate::INDEX_KEY_SIZE];
            item[..$prop.len()].copy_from_slice($prop);
            item[crate::SORT_KEY_SIZE..crate::INDEX_KEY_SIZE].copy_from_slice(&$uuid);

            item
        }
    };
}

#[macro_export]
macro_rules! create_index_item {
    ($prop:expr) => {
        {
            let uuid = crate::uuid!();
            crate::index_item!(uuid, $prop)
        }
    };
}

#[macro_export]
macro_rules! create_index_item_uuid {
    ($prop:expr) => {
        {
            let uuid = crate::uuid!();
            (crate::index_item!(uuid, $prop), uuid)
        }
    };
}