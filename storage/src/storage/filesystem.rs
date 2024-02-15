use std::path::PathBuf;

use super::storage::Storage;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
}

pub struct FileSystem {
    root: PathBuf,
}

impl Storage for FileSystem {
    type Error = Error;

    fn list_objects(
        &self,
        params: super::storage::StorageListObjectsParams,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<String>, Self::Error>> + Send + '_>,
    > {
        todo!()
    }

    fn put_object(
        &self,
        buffer: Vec<u8>,
        key: impl Into<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + '_>>
    {
        todo!()
    }

    fn get_object(
        &self,
        key: impl Into<String>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, Self::Error>> + Send + '_>,
    > {
        todo!()
    }

    fn delete_object(
        &self,
        key: impl Into<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + '_>>
    {
        todo!()
    }
}
