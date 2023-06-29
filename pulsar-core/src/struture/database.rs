use crate::filter_and_push;
use std::collections::HashMap;

use super::{table::Table, Error, Filter, ListProps, Order, PartitionTrait, StartAfter};

// Create Database struct
pub struct Database<'a> {
    map: HashMap<&'a str, Table<'a>>,
    list: Vec<&'a str>,
    capacity: usize,
    table_capacity: usize,
    partition_capacity: usize,
}

impl<'a> Database<'a> {
    pub fn new(capacity: usize, table_capacity: usize, partition_capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            list: Vec::new(),
            capacity: 10,
            table_capacity: 10,
            partition_capacity: 10,
        }
    }

    pub fn create(&mut self, key: &'a str) -> Result<(), Error> {
        if self.map.contains_key(key) {
            return Err(Error::TableAlreadyExists);
        }

        self.insert(
            key,
            Table::new(self.table_capacity, self.partition_capacity),
        );
        Ok(())
    }
}

impl<'a> PartitionTrait<'a> for Database<'a> {
    type Output = Table<'a>;

    fn insert<V>(&mut self, key: &'a str, value: V)
    where
        V: Into<Self::Output>,
    {   
        if self.map.len() != 0 && self.map.len() == self.capacity {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create() {
        let mut db = Database::new(10, 5, 5);
        assert!(db.create("table1").is_ok());
        assert!(db.create("table2").is_ok());
        assert_eq!(db.map.len(), 2);
        assert!(db.create("table1").is_err());
        assert_eq!(db.map.len(), 2);
    }

    #[test]
    fn test_insert() {
        let mut db = Database::new(10, 5, 5);
        let table1 = Table::new(5, 5);
        let table2 = Table::new(5, 5);
        let table3 = Table::new(5, 5);

        db.insert("table1", table1.clone());
        db.insert("table2", table2.clone());
        db.insert("table3", table3.clone());

        assert_eq!(db.get("table1"), Some(&table1));
        assert_eq!(db.get("table2"), Some(&table2));
        assert_eq!(db.get("table3"), Some(&table3));
    }

    #[test]
    fn test_insert_if_not_exists() {
        let mut db = Database::new(10, 5, 5);
        let table1 = Table::new(5, 5);
        let table2 = Table::new(5, 5);

        assert!(db.insert_if_not_exists("table1", table1.clone()).is_ok());
        assert!(db.insert_if_not_exists("table2", table2.clone()).is_ok());
        assert_eq!(db.map.len(), 2);
        assert!(db.insert_if_not_exists("table1", table1.clone()).is_err());
        assert_eq!(db.map.len(), 2);
    }

    #[test]
    fn test_remove() {
        let mut db = Database::new(10, 5, 5);
        let table1 = Table::new(5, 5);
        let table2 = Table::new(5, 5);
        let table3 = Table::new(5, 5);

        db.insert("table1", table1.clone());
        db.insert("table2", table2.clone());
        db.insert("table3", table3.clone());

        db.remove("table2");

        assert_eq!(db.get("table1"), Some(&table1));
        assert_eq!(db.get("table2"), None);
        assert_eq!(db.get("table3"), Some(&table3));
    }
}
