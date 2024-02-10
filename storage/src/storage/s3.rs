use std::{future::Future, pin::Pin};

use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::{
    error::SdkError,
    operation::{list_objects_v2::ListObjectsV2Error, put_object::PutObjectError},
    primitives::ByteStream,
    Client,
};

use super::storage::{Storage, StorageListObjectsParams};

pub enum Error {
    GetObject(SdkError<aws_sdk_s3::operation::get_object::GetObjectError>),
    DeleteObject(SdkError<aws_sdk_s3::operation::delete_object::DeleteObjectError>),
    ListObject(SdkError<ListObjectsV2Error>),
    PutObject(SdkError<PutObjectError>),
    Io(std::io::Error),
}

pub struct S3 {
    bucket: String,
    client: Client,
}

impl Storage for S3 {
    type Error = Error;

    /// Create a new instance of `Bucket` with the given bucket name.
    fn builder(bucket: String) -> Pin<Box<dyn Future<Output = Self> + Send>> {
        Box::pin(async move {
            let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region_provider)
                .load()
                .await;
            let client = Client::new(&config);

            Self {
                bucket: bucket,
                client,
            }
        })
    }

    /// List all objects in the bucket.
    fn list_objects(
        &self,
        params: StorageListObjectsParams,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, Error>> + Send + '_>> {
        Box::pin(async move {
            let mut builder = self.client.list_objects_v2().bucket(&self.bucket);

            if let Some(max_keys) = params.max_keys {
                builder = builder.max_keys(max_keys);
            }

            if let Some(prefix) = params.prefix {
                builder = builder.prefix(prefix);
            }

            if let Some(delimiter) = params.delimiter {
                builder = builder.delimiter(delimiter);
            }

            if let Some(start_after) = params.start_after {
                builder = builder.start_after(start_after);
            }

            let mut response = builder.into_paginator().send();

            let mut keys = Vec::new();

            while let Some(result) = response.next().await {
                match result {
                    Ok(output) => {
                        for object in output.contents() {
                            if let Some(key) = object.key() {
                                keys.push(key.to_string());
                            }
                        }
                    }
                    Err(err) => {
                        return Err(Error::ListObject(err));
                    }
                }
            }

            Ok(keys)
        })
    }

    /// Put an object in the bucket from a buffer.
    fn put_object(
        &self,
        buffer: Vec<u8>,
        key: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(async move {
            let body = ByteStream::from(buffer);
            match self
                .client
                .put_object()
                .bucket(&self.bucket)
                .key(key)
                .body(body)
                .send()
                .await
            {
                Ok(output) => Ok(()),
                Err(err) => Err(Error::PutObject(err)),
            }
        })
    }

    /// Get an object from the bucket as a buffer.
    fn get_object(
        &self,
        key: String,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, Self::Error>> + Send + '_>> {
        Box::pin(async move {
            let mut object = match self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(output) => output,
                Err(err) => return Err(Error::GetObject(err)),
            };

            let mut buffer = Vec::new();
            while let Some(bytes) = object.body.try_next().await.unwrap_or(None) {
                buffer.extend_from_slice(&bytes);
            }

            Ok(buffer)
        })
    }

    /// Delete an object from the bucket.
    fn delete_object(
        &self,
        key: String,
    ) -> Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send + '_>> {
        Box::pin(async move {
            match self
                .client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
            {
                Ok(_) => Ok(()),
                Err(err) => Err(Error::DeleteObject(err)),
            }
        })
    }
}
