use std::collections::HashMap;
use valu3::prelude::*;

enum Error {
    NonParsebleMsg(String),
    NonParseble,
    NotNumber,
    SortKeyNotFound,
}

struct Partition<'a> {
    map: HashMap<&'a str, Value>,
    list: Vec<&'a str>,
    capacity: usize,
}

impl<'a> Partition<'a> {
    fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            list: Vec::new(),
            capacity,
        }
    }

    fn insert(&mut self, key: &'a str, value: Value) {
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
        self.map.insert(key, value);
    }

    fn get(&self, key: &str) -> Option<&Value> {
        self.map.get(key)
    }

    fn remove(&mut self, key: &str) {
        let position = self.list.iter().position(|&k| k == key).unwrap();

        self.list.remove(position);
        self.map.remove(key);
    }

    fn clear(&mut self) {
        self.map.clear();
        self.list.clear();
    }

    fn len(&self) -> usize {
        self.map.len()
    }

    fn capacity(&self) -> usize {
        self.capacity
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    fn iter_from_asc(&self, start_after_key: &str) -> Result<Vec<(&str, &Value)>, Error> {
        if !self.map.contains_key(start_after_key) {
            return Err(Error::SortKeyNotFound);
        }

        let position = match self.list.iter().position(|&k| k == start_after_key) {
            Some(position) => position + 1,
            None => return Err(Error::SortKeyNotFound),
        };

        let result = self
            .list
            .iter()
            .skip(position)
            .map(|&k| (k, self.map.get(k).unwrap()))
            .collect();

        Ok(result)
    }

    fn iter_from_desc(&self, start_after_key: &str) -> Result<Vec<(&str, &Value)>, Error> {
        if !self.map.contains_key(start_after_key) {
            return Err(Error::SortKeyNotFound);
        }

        let position = match self.list.iter().position(|&k| k == start_after_key) {
            Some(position) => position + 1,
            None => return Err(Error::SortKeyNotFound),
        };

        let result = self
            .list
            .iter()
            .rev()
            .skip(position)
            .map(|&k| (k, self.map.get(k).unwrap()))
            .collect();

        Ok(result)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_partition_insert() {
        let mut partition = Partition::new(2);
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.insert("key3", Value::from(3));
        assert_eq!(partition.get("key1"), None);
        assert_eq!(partition.get("key2"), Some(&Value::from(2)));
        assert_eq!(partition.get("key3"), Some(&Value::from(3)));
    }

    #[test]
    fn test_partition_remove() {
        let mut partition = Partition::new(2);
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.remove("key1");
        assert_eq!(partition.get("key1"), None);
        partition.insert("key3", Value::from(3));
        assert_eq!(partition.get("key3"), Some(&Value::from(3)));
        assert_eq!(partition.get("key2"), Some(&Value::from(2)));
    }

    #[test]
    fn test_partition_clear() {
        let mut partition = Partition::new(2);
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.clear();
        assert_eq!(partition.len(), 0);
    }

    #[test]
    fn test_partition_iter_from_asc() {
        let mut partition = Partition::new(5);
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.insert("key3", Value::from(3));
        partition.insert("key4", Value::from(4));
        partition.insert("key5", Value::from(5));

        let iter_result = partition.iter_from_asc("key2");

        assert_eq!(iter_result.is_ok(), true);

        let result = match iter_result {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("key3", &Value::from(3)));
        assert_eq!(result[1], ("key4", &Value::from(4)));
        assert_eq!(result[2], ("key5", &Value::from(5)));
    }

    #[test]
    fn test_partition_iter_from_desc() {
        let mut partition = Partition::new(5);
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.insert("key3", Value::from(3));
        partition.insert("key4", Value::from(4));
        partition.insert("key5", Value::from(5));

        let iter_result = partition.iter_from_desc("key3");

        assert_eq!(iter_result.is_ok(), true);

        let result = match iter_result {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("key2", &Value::from(2)));
        assert_eq!(result[1], ("key1", &Value::from(1)));
    }

    #[test]
    fn test_partition_iter_from_asc_alphabetic() {
        let mut partition = Partition::new(5);
        partition.insert("key2", Value::from(2));
        partition.insert("key5", Value::from(5));
        partition.insert("key1", Value::from(1));
        partition.insert("key3", Value::from(3));
        partition.insert("key4", Value::from(4));

        let iter_result = partition.iter_from_asc("key1");

        assert_eq!(iter_result.is_ok(), true);

        let result = match iter_result {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 4);
        assert_eq!(result[0], ("key2", &Value::from(2)));
        assert_eq!(result[1], ("key3", &Value::from(3)));
        assert_eq!(result[2], ("key4", &Value::from(4)));
        assert_eq!(result[3], ("key5", &Value::from(5)));
    }

    #[test]
    fn test_partition_iter_from_desc_alphabetic() {
        let mut partition = Partition::new(5);
        partition.insert("key4", Value::from(4));
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.insert("key5", Value::from(5));
        partition.insert("key3", Value::from(3));

        let iter_result = partition.iter_from_desc("key3");

        assert_eq!(iter_result.is_ok(), true);

        let result = match iter_result {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("key2", &Value::from(2)));
        assert_eq!(result[1], ("key1", &Value::from(1)));
    }
}
