use std::collections::HashMap;
use valu3::value::Value;
use crate::partition::{Partition, Error, ListProps};

pub struct Table<'a> {
    partition: Partition<'a>,
}

impl<'a> Table<'a> {
    fn new(capacity: usize) -> Self {
        Self {
            partition: Partition::new(capacity),
        }
    }

    fn insert(&mut self, key: &'a str, value: Partition<'a>)  {
        self.partition.insert(key, value)
    }
}