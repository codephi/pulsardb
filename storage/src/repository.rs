use crate::storage::storage::{Storage, StorageListObjectsParams};
pub enum Error<S>
where
    S: Storage,
{
    Storage(S::Error),
}

pub struct Repository<S>
where
    S: Storage,
{
    storage: S,
    default_max_keys: i32,
}

impl<S> Repository<S>
where
    S: Storage,
{
    pub async fn builder(storage: S, default_max_keys: i32) -> Self {
        Self {
            storage,
            default_max_keys,
        }
    }

    fn get_key(
        &self,
        collection: &str,
        table_name: &str,
        partition_key: &str,
        sort_key: &str,
    ) -> String {
        format!(
            "collection={}/table={}/partition_key={}/sort_key={}.parquet",
            collection, table_name, partition_key, sort_key,
        )
    }

    pub async fn list_items(
        &self,
        collection: &str,
        table_name: &str,
        params: ListObjectsParams,
    ) -> Result<Vec<String>, Error<S>> {
        let mut prefix = format!("collection={}/table={}", collection, table_name);

        if let Some(partition_key) = params.partition_key {
            prefix.push_str(&format!("/partition_key={}", partition_key));

            if let Some(sort_key) = params.sort_key {
                prefix.push_str(&format!("/sort_key={}", sort_key));
            }
        }

        let params_list_objects: StorageListObjectsParams = StorageListObjectsParams {
            max_keys: params.max_keys.into(),
            delimiter: params.delimiter.into(),
            start_after: params.start_after.into(),
            prefix: Some(prefix.into()),
        };

        match self.storage.list_objects(params_list_objects).await {
            Ok(keys) => Ok(keys),
            Err(err) => Err(Error::Storage(err)),
        }
    }
}

#[derive(Debug)]
pub struct ListObjectsParams {
    pub max_keys: Option<i32>,
    pub partition_key: Option<String>,
    pub sort_key: Option<String>,
    pub delimiter: Option<String>,
    pub start_after: Option<String>,
}
