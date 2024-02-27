use std::path::PathBuf;

use sinfonia_sdk::{Order, Storage, StorageListObjectsParams, DEFAULT_MAX_KEYS};

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
        let root = PathBuf::from(root_path);

        Ok(Box::pin(async move {
            // create recursive directory
            match std::fs::create_dir_all(&root) {
                Ok(_) => Ok(FileSystem { root }),
                Err(e) => Err(Error::Io(e)),
            }
        }))
    }

    fn list_objects(
        &self,
        params: StorageListObjectsParams,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<String>, Self::Error>> + Send + '_>,
    > {
        let root = self.root.clone();

        Box::pin(async move {
            let mut entries = match std::fs::read_dir(&root) {
                Ok(entries) => {
                    let mut entries: Vec<_> = entries.collect();

                    if let Some(order) = &params.order {
                        if order.eq(&Order::Desc) {
                            entries.reverse();
                        }
                    }

                    entries.into_iter()
                }
                Err(e) => return Err(Error::Io(e)),
            };

            if let Some(start_after) = &params.start_after {
                while let Some(entry) = entries.next() {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(e) => return Err(Error::Io(e)),
                    };

                    let path = entry.path();

                    if let Some(file_name) = path.file_name() {
                        let file_name = file_name.to_string_lossy().to_string();

                        if file_name.ne(start_after) {
                            continue;
                        }

                        break;
                    }
                }
            }

            let mut keys = Vec::new();

            while let Some(entry) = entries.next() {
                let entry = match entry {
                    Ok(entry) => entry,
                    Err(e) => return Err(Error::Io(e)),
                };

                let path = entry.path();

                if let Some(file_name) = path.file_name() {
                    let file_name = file_name.to_string_lossy().to_string();

                    if let Some(prefix) = &params.prefix {
                        if !file_name.starts_with(prefix) {
                            continue;
                        }
                    }

                    if let Some(delimiter) = &params.delimiter {
                        if file_name.contains(delimiter) {
                            continue;
                        }
                    }

                    keys.push(file_name);

                    if keys.len() == params.max_keys.unwrap_or(DEFAULT_MAX_KEYS) as usize {
                        break;
                    }
                }
            }

            Ok(keys)
        })
    }

    fn put_object(
        &self,
        buffer: Vec<u8>,
        key: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + '_>>
    {
        let path = self.root.join(key);

        Box::pin(async move {
            match std::fs::write(&path, buffer) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Io(e)),
            }
        })
    }

    fn get_object(
        &self,
        key: &str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Vec<u8>, Self::Error>> + Send + '_>,
    > {
        let path = self.root.join(key);

        Box::pin(async move {
            match std::fs::read(&path) {
                Ok(buffer) => Ok(buffer),
                Err(e) => Err(Error::Io(e)),
            }
        })
    }

    fn delete_object(
        &self,
        key: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Self::Error>> + Send + '_>>
    {
        let path = self.root.join(key);

        Box::pin(async move {
            match std::fs::remove_file(&path) {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::Io(e)),
            }
        })
    }
}


#[cfg(test)]
mod tests {
    use sinfonia_sdk::Order;

    use super::*;

    #[tokio::test]
    async fn test_create_filesystem() {
        let path = "/tmp/sinfonia/test_create_filesystem";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        assert_eq!(storage.root, PathBuf::from(path));

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_put_get_and_delete_object() {
        let path = "/tmp/sinfonia/test_put_get_and_delete_object";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        let key = "test.txt";
        let buffer = b"Hello, World!".to_vec();

        storage.put_object(buffer.clone(), key).await.unwrap();

        let result = storage.get_object(key).await.unwrap();

        assert_eq!(result, buffer);

        storage.delete_object(key).await.unwrap();

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_list_objects() {
        let path = "/tmp/sinfonia/test_list_objects";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        let keys = vec!["test1.txt", "test2.txt", "test3.txt"];

        for key in &keys {
            storage
                .put_object(b"Hello, World!".to_vec(), key)
                .await
                .unwrap();
        }

        let result = storage
            .list_objects(StorageListObjectsParams {
                max_keys: None,
                prefix: None,
                delimiter: None,
                start_after: None,
                order: None,
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 3);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let path = "/tmp/sinfonia/test_list_objects_with_prefix";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        for key in 0..100 {
            storage
                .put_object(b"Hello, World!".to_vec(), &format!("file{}.txt", key))
                .await
                .unwrap();
        }

        let result = storage
            .list_objects(StorageListObjectsParams {
                max_keys: None,
                prefix: Some("file1".to_string()),
                delimiter: None,
                start_after: None,
                order: None,
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 11);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_list_objects_with_max_keys() {
        let path = "/tmp/sinfonia/test_list_objects_with_max_keys";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        for key in 0..100 {
            storage
                .put_object(b"Hello, World!".to_vec(), &format!("file{}.txt", key))
                .await
                .unwrap();
        }

        let result = storage
            .list_objects(StorageListObjectsParams {
                max_keys: Some(10),
                prefix: None,
                delimiter: None,
                start_after: None,
                order: None,
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 10);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_list_objects_with_start_after() {
        let path = "/tmp/sinfonia/test_list_objects_with_start_after";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        for key in 0..100 {
            storage
                .put_object(b"Hello, World!".to_vec(), &format!("file{}.txt", key))
                .await
                .unwrap();
        }

        let result = storage
            .list_objects(StorageListObjectsParams {
                max_keys: None,
                prefix: None,
                delimiter: None,
                start_after: Some("file10.txt".to_string()),
                order: None,
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 89);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_list_objects_with_delimiter() {
        let path = "/tmp/sinfonia/test_list_objects_with_delimiter";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        for pk in 0..100 {
            for sk in 0..10 {
                storage
                    .put_object(b"Hello, World!".to_vec(), &format!("pk{}:sk{}", pk, sk))
                    .await
                    .unwrap();
            }
        }

        let result = storage
            .list_objects(StorageListObjectsParams {
                max_keys: None,
                prefix: None,
                delimiter: Some("9:".to_string()),
                start_after: None,
                order: None,
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 100);

        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn test_list_objects_with_order() {
        let path = "/tmp/sinfonia/test_list_objects_with_order";

        let storage = FileSystem::try_builder(path).unwrap().await.unwrap();

        for pk in 0..100 {
            for sk in 0..10 {
                storage
                    .put_object(b"Hello, World!".to_vec(), &format!("pk{}:sk{}", pk, sk))
                    .await
                    .unwrap();
            }
        }

        let result = storage
            .list_objects(StorageListObjectsParams {
                max_keys: None,
                prefix: None,
                delimiter: None,
                start_after: None,
                order: Some(Order::Asc),
            })
            .await
            .unwrap();

        assert_eq!(result.get(0), Some(&"pk0:sk0".to_string()));

        let result = storage
            .list_objects(StorageListObjectsParams {
                max_keys: None,
                prefix: None,
                delimiter: None,
                start_after: None,
                order: Some(Order::Desc),
            })
            .await
            .unwrap();

        assert_eq!(result.get(0), Some(&"pk99:sk9".to_string()));

        std::fs::remove_dir_all(path).unwrap();
    }
}
