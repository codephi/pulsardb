use std::collections::HashMap;
use valu3::value::Value;
use crate::partition::{Partition, Error, ListProps};

pub struct Table<'a> {
    partitions: HashMap<&'a str, Partition<'a>>,
}

impl<'a> Table<'a> {
    pub fn new() -> Self {
        Self {
            partitions: HashMap::new(),
        }
    }

    pub fn create_partition(&mut self, name: &'a str, capacity: usize) -> Result<(), Error> {
        if self.partitions.contains_key(name) {
            return Err(Error::PartitionAlreadyExists);
        }
        self.partitions.insert(name, Partition::new(capacity));
        Ok(())
    }

    pub fn get_partition(&self, name: &'a str) -> Option<&Partition<'a>> {
        self.partitions.get(name)
    }

    pub fn get_partition_mut(&mut self, name: &'a str) -> Option<&mut Partition<'a>> {
        self.partitions.get_mut(name)
    }

    pub fn remove_partition(&mut self, name: &'a str) -> Option<Partition<'a>> {
        self.partitions.remove(name)
    }

    pub fn list_partitions(&self) -> HashMap<&'a str, Partition<'a>> {
        self.partitions.clone()
    }
}