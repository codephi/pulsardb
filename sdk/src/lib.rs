use std::{future::Future, pin::Pin};

pub const DEFAULT_MAX_KEYS: i32 = 100;

#[derive(Debug, PartialEq)]
pub enum Order {
    Asc,
    Desc
}

#[derive(Debug)]
pub struct StorageListObjectsParams {
    /// The maximum number of keys to return.
    pub max_keys: Option<i32>,
    /// Limits the response to keys that begin with the specified prefix.
    pub prefix: Option<String>,
    /// A delimiter is a character you use to group keys.
    pub delimiter: Option<String>,
    /// Specifies the key to start with when listing objects in a bucket.
    pub start_after: Option<String>,
    /// Specifies the order for listing objects.
    pub order: Option<Order>,
}

pub trait Storage {
    type Error;

    fn try_builder<'a>(param: &'a str) -> Result<Pin<Box<dyn Future<Output = Result<Self, Self::Error>> + Send>>, Self::Error>
    where
        Self: Sized;
    
    fn list_objects(
        &self,
        params: StorageListObjectsParams,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, Self::Error>> + Send + '_>>;
    fn put_object<'a>(
        &self,
        buffer: Vec<u8>,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;
    fn get_object<'a>(
        &self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Self::Error>> + Send + '_>>;
    fn delete_object<'a>(
        &self,
        key: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;
}
