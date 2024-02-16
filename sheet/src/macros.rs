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
