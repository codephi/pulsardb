use std::{future::Future, pin::Pin};

#[derive(Debug)]
pub struct StorageListObjectsParams {
    pub max_keys: Option<i32>,
    pub prefix: Option<String>,
    pub delimiter: Option<String>,
    pub start_after: Option<String>,
}

pub trait Storage {
    type Error;

    fn builder(bucket: String) -> Pin<Box<dyn Future<Output = Self> + Send>>;
    fn list_objects(
        &self,
        params: StorageListObjectsParams,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, Self::Error>> + Send + '_>>;
    fn put_object(
        &self,
        buffer: Vec<u8>,
        key: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;
    fn get_object(
        &self,
        key: String,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Self::Error>> + Send + '_>>;
    fn delete_object(
        &self,
        key: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>>;
}
