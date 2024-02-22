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

    fn try_builder(
        root_path: &str,
    ) -> Result<
        std::pin::Pin<Box<dyn std::future::Future<Output = Result<Self, Self::Error>> + Send>>,
        Self::Error,
    >
    where
        Self: Sized,
    {
        Ok(Box::pin(async move {
            let root = PathBuf::from(root_path);

            // create recursive directory
            match std::fs::create_dir_all(&root) {
                Ok(_) => Ok(FileSystem { root }),
                Err(e) => Err(Error::Io(e)),
            }
        }))
    }

    fn list_objects(
        &self,
        params: super::storage::StorageListObjectsParams,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<String>, Self::Error>> + Send + '_>,
    > {
        Box::pin(async move {
            let mut keys = Vec::new();
            let mut entries = match std::fs::read_dir(&self.root) {
                Ok(entries) => entries,
                Err(e) => return Err(Error::Io(e)),
            };

            while let Some(entry) = entries.next() {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => return Err(Error::Io(e)),
                };

                let path = entry.path();
                let key = match path.file_name() {
                    Some(key) => key.to_string_lossy().to_string(),
                    None => continue,
                };

                keys.push(key);
            }

            Ok(keys)
        })
    }

    fn put_object(
        &self,
        buffer: Vec<u8>,
        key: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + '_>>
    {
        todo!()
    }

    fn get_object(
        &self,
        key: String,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, Self::Error>> + Send + '_>,
    > {
        todo!()
    }

    fn delete_object(
        &self,
        key: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + '_>>
    {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_filesystem() {
        let path= "/tmp/sinfonia/create_filesystem";
        
        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        assert_eq!(storage.root, PathBuf::from(path));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn list_objects() {
        let path= "/tmp/sinfonia/list_objects";
        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        let keys = storage.list_objects(super::super::storage::StorageListObjectsParams {
            max_keys: None,
            prefix: None,
            delimiter: None,
            start_after: None,
        }).await.unwrap();

        assert_eq!(keys, Vec::<String>::new());

        std::fs::remove_dir_all(path).unwrap();
    }
}