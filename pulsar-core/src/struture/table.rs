use std::collections::HashMap;
use crate::filter_and_push;

use super::{PartitionTrait, Error, ListProps, StartAfter, Order, partition::Partition, Filter};

#[derive(Clone, Debug, PartialEq)]
pub struct Table<'a> {
    map: HashMap<&'a str, Partition<'a>>,
    list: Vec<&'a str>,
    capacity: usize,
    partition_capacity: usize,
}

impl<'a> Table<'a> {
    pub fn new(capacity: usize, partition_capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            list: Vec::new(),
            capacity,
            partition_capacity,
        }
    }

    pub fn create(&mut self, key: &'a str) -> Result<(), Error> {
        if self.map.contains_key(key) {
            return Err(Error::PartitionAlreadyExists);
        }

        self.insert(key, Partition::new(self.partition_capacity));
        Ok(())
    }
}

impl<'a> PartitionTrait<'a> for Table<'a> {
    type Output = Partition<'a>;

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
            return Err(Error::PartitionAlreadyExists);
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

    fn clear(&mut self) {
        self.map.clear();
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
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
    fn test_insert_and_get() {
        let mut table = Table::new(10, 5);
        let partition = Partition::new(5);
        table.insert("key1", partition.clone());
        assert_eq!(table.get("key1"), Some(&partition));
    }

    #[test]
    fn test_remove() {
        let mut table = Table::new(10, 5);
        let partition = Partition::new(5);
        table.insert("key1", partition.clone());
        table.remove("key1");
        assert_eq!(table.get("key1"), None);
    }

    #[test]
    fn test_clear() {
        let mut table = Table::new(10, 5);
        let partition = Partition::new(5);
        table.insert("key1", partition.clone());
        table.clear();
        assert_eq!(table.len(), 0);
    }

    #[test]
    fn test_list() {
        let mut table = Table::new(10, 5);
        let partition1 = Partition::new(5);
        let partition2 = Partition::new(5);

        table.insert("key1", partition1.clone());
        table.insert("key2", partition2.clone());

        match table.list(ListProps::default()) {
            Ok(table) => table,
            Err(err) => panic!("Error: {:?}", err),
        };

        assert_eq!(table.len(), 2);

        assert!(table.contains_key("key1"));
        assert!(table.contains_key("key2"));
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_clear() {
        let mut table = Table::new(10, 5);
        table.create("key1");
        table.clear();
        assert_eq!(table.len(), 0);
        assert_eq!(table.get("key1"), None);
    }

    #[test]
    fn test_contains_key() {
        let mut table = Table::new(10, 5);
        table.create("key1");
        assert_eq!(table.contains_key("key1"), true);
        assert_eq!(table.contains_key("key2"), false);
    }

    #[test]
    fn test_is_empty() {
        let mut table = Table::new(10, 5 );
        assert_eq!(table.is_empty(), true);
        table.create("key1");
        assert_eq!(table.is_empty(), false);
    }

    #[test]
    fn test_list() {
        let mut table = Table::new(10, 5);
        table.create("key1");
        table.create("key2");
        let list = table.list(ListProps::default()).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].0, "key1");
        assert_eq!(list[1].0, "key2");
    }

    #[test]
    fn test_list_with_start_after_key() {
        let mut table = Table::new(10, 5);
        table.create("key1");
        table.create("key2");
        let list = table
            .list(ListProps {
                start_after_key: StartAfter::Key("key1"),
                ..ListProps::default()
            })
            .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "key2");
        assert_eq!(list[0].1, &Partition::new(5));
    }
}
