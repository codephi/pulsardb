use std::collections::HashMap;
use crate::filter_and_push;

use super::{PartitionTrait, Error, ListProps, StartAfter, Order, table::Table, Filter};

// Create Database struct
pub struct Database<'a> {
    pub name: String,
    pub map: HashMap<&'a str, Table<'a>>,
    pub list: Vec<&'a str>,
    pub capacity: usize,
    pub table_capacity: usize,
    pub partition_capacity: usize,
}

impl<'a> Database<'a> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            map: HashMap::new(),
            list: Vec::new(),
            capacity: 0,
            table_capacity: 0,
            partition_capacity: 0,
        }
    }

    pub fn create(&mut self, key: &'a str) -> Result<(), Error> {
        if self.map.contains_key(key) {
            return Err(Error::TableAlreadyExists);
        }

        self.insert(key, Table::new(self.table_capacity, self.partition_capacity));
        Ok(())
    }
}

impl<'a> PartitionTrait<'a> for Database<'a> {
    type Output = Table<'a>;

    fn insert<V>(&mut self, key: &'a str, value: V)
    where
        V: Into<Self::Output>,
    {
        if self.map.len() == self.capacity {
            let first_key = self.list.remove(0);
            self.map.remove(first_key);
        }

        // sorted insert
        let position = self
            .list
            .iter()
            .position(|&k| k > key)
            .unwrap_or(self.list.len());
        self.list.insert(position, key);
        self.map.insert(key, value.into());
    }

    fn insert_if_not_exists<V>(&mut self, key: &'a str, value: V) -> Result<(), Error>
    where
        V: Into<Self::Output>,
    {
        if self.map.contains_key(key) {
            return Err(Error::TableAlreadyExists);
        }

        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &str) -> Option<&Self::Output> {
        self.map.get(key)
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut Self::Output> {
        self.map.get_mut(key)
    }

    fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn clear(&mut self) {
        self.map.clear();
    }

    fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    fn list<T>(&self, props: T) -> Result<Vec<(&str, &Self::Output)>, Error>
    where
        T: Into<ListProps<'a>>,
    {
        let props = props.into();

        let position = match props.start_after_key {
            StartAfter::Key(key) => {
                self.list
                    .iter()
                    .position(|&k| k == key)
                    .ok_or(Error::PartitionAlreadyExists)?
                    + 1
            }
            StartAfter::None => 0,
        };

        let mut list = Vec::new();
        let mut count = 0;

        match props.order {
            Order::Asc => {
                let skip_iter = self.list.iter().skip(position);
                filter_and_push!(
                    props.filter,
                    self.map,
                    list,
                    count,
                    props.limit,
                    filter_fn,
                    skip_iter
                );
            }
            Order::Desc => {
                let skip_iter = self.list.iter().rev().skip(position);
                filter_and_push!(
                    props.filter,
                    self.map,
                    list,
                    count,
                    props.limit,
                    filter_fn,
                    skip_iter
                );
            }
        };

        Ok(list)
    }
}
