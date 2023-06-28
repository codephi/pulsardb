use std::collections::HashMap;
use valu3::prelude::*;

enum Error {
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

    fn list_asc(&self, start_after_key: &str) -> Result<Vec<(&str, &Value)>, Error> {
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

    fn list_desc(&self, start_after_key: &str) -> Result<Vec<(&str, &Value)>, Error> {
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

    fn filter_starts_with_and_start_after(
        &self,
        start_after_key: Option<&str>,
        key_start_with: &str,
    ) -> Result<Vec<(&str, &Value)>, Error> {
        let position = match start_after_key {
            Some(start_after_key) => {
                if !self.map.contains_key(start_after_key) {
                    return Err(Error::SortKeyNotFound);
                }

                match self.list.iter().position(|&k| k == start_after_key) {
                    Some(position) => position + 1,
                    None => return Err(Error::SortKeyNotFound),
                }
            }
            None => 0,
        };

        let result = self
            .list
            .iter()
            .skip(position)
            .filter_map(|&k| {
                if k.starts_with(key_start_with) {
                    Some((k, self.map.get(k).unwrap()))
                } else {
                    None
                }
            })
            .collect();

        Ok(result)
    }

    fn filter_ends_with_and_start_after(
        &self,
        start_after_key: Option<&str>,
        key_ends_with: &str,
    ) -> Result<Vec<(&str, &Value)>, Error> {
        let position = match start_after_key {
            Some(start_after_key) => {
                if !self.map.contains_key(start_after_key) {
                    return Err(Error::SortKeyNotFound);
                }

                match self.list.iter().position(|&k| k == start_after_key) {
                    Some(position) => position + 1,
                    None => return Err(Error::SortKeyNotFound),
                }
            }
            None => 0,
        };

        let result = self
            .list
            .iter()
            .skip(position)
            .filter_map(|&k| {
                if k.ends_with(key_ends_with) {
                    Some((k, self.map.get(k).unwrap()))
                } else {
                    None
                }
            })
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
    fn test_partition_list_asc() {
        let mut partition = Partition::new(5);
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.insert("key3", Value::from(3));
        partition.insert("key4", Value::from(4));
        partition.insert("key5", Value::from(5));

        let result_res = partition.list_asc("key2");

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("key3", &Value::from(3)));
        assert_eq!(result[1], ("key4", &Value::from(4)));
        assert_eq!(result[2], ("key5", &Value::from(5)));
    }

    #[test]
    fn test_partition_list_desc() {
        let mut partition = Partition::new(5);
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.insert("key3", Value::from(3));
        partition.insert("key4", Value::from(4));
        partition.insert("key5", Value::from(5));

        let result_res = partition.list_desc("key3");

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("key2", &Value::from(2)));
        assert_eq!(result[1], ("key1", &Value::from(1)));
    }

    #[test]
    fn test_partition_list_asc_alphabetic() {
        let mut partition = Partition::new(5);
        partition.insert("key2", Value::from(2));
        partition.insert("key5", Value::from(5));
        partition.insert("key1", Value::from(1));
        partition.insert("key3", Value::from(3));
        partition.insert("key4", Value::from(4));

        let result_res = partition.list_asc("key1");

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
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
    fn test_partition_list_desc_alphabetic() {
        let mut partition = Partition::new(5);
        partition.insert("key4", Value::from(4));
        partition.insert("key1", Value::from(1));
        partition.insert("key2", Value::from(2));
        partition.insert("key5", Value::from(5));
        partition.insert("key3", Value::from(3));

        let result_res = partition.list_desc("key3");

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("key2", &Value::from(2)));
        assert_eq!(result[1], ("key1", &Value::from(1)));
    }

    #[test]
    fn test_filter_start_with() {
        let mut partition = Partition::new(10);

        partition.insert("postmodern", Value::from(8));
        partition.insert("postpone", Value::from(6));
        partition.insert("precept", Value::from(2));
        partition.insert("postmortem", Value::from(9));
        partition.insert("precaution", Value::from(3));
        partition.insert("precede", Value::from(1));
        partition.insert("precognition", Value::from(5));
        partition.insert("postmark", Value::from(10));
        partition.insert("postgraduate", Value::from(7));
        partition.insert("preconceive", Value::from(4));

        let result_res = partition.filter_starts_with_and_start_after(None, "postm");

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("postmark", &Value::from(10)));
        assert_eq!(result[1], ("postmodern", &Value::from(8)));
        assert_eq!(result[2], ("postmortem", &Value::from(9)));
    }

    #[test]
    fn test_filter_ends_with() {
        let mut partition = Partition::new(10);

        partition.insert("postmodern", Value::from(8));
        partition.insert("postpone", Value::from(6));
        partition.insert("precept", Value::from(2));
        partition.insert("postmortem", Value::from(9));
        partition.insert("precaution", Value::from(3));
        partition.insert("precede", Value::from(1));
        partition.insert("precognition", Value::from(5));
        partition.insert("postmark", Value::from(10));
        partition.insert("postgraduate", Value::from(7));
        partition.insert("preconceive", Value::from(4));

        let result_res = partition.filter_ends_with_and_start_after(None, "tion");

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("precaution", &Value::from(3)));
        assert_eq!(result[1], ("precognition", &Value::from(5)));
    }
}
