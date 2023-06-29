use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Display;
pub use valu3::prelude::*;

pub enum Error {
    SortKeyNotFound,
    CacheAlreadyExists,
    SortKeyExists,
    TableAlreadyExists,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::SortKeyNotFound => write!(f, "Sort key not found"),
            Error::CacheAlreadyExists => write!(f, "Cache already exists"),
            Error::SortKeyExists => write!(f, "Sort key exists"),
            Error::TableAlreadyExists => write!(f, "Table already exists"),
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

#[derive(Debug, Clone)]
pub enum Filter {
    StartWith(String),
    EndWith(String),
    StartAndEndWith(String, String),
    None,
}

impl Default for Filter {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone)]
pub enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Asc
    }
}

#[derive(Debug, Clone)]
pub enum StartAfter {
    Key(String),
    None,
}

impl Default for StartAfter {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default, Debug, Clone)]
pub struct ListProps {
    pub start_after_key: StartAfter,
    pub filter: Filter,
    pub order: Order,
    pub limit: usize,
}

impl ListProps {
    fn new() -> Self {
        Self {
            start_after_key: StartAfter::None,
            filter: Filter::None,
            order: Order::Asc,
            limit: 10,
        }
    }

    pub fn start_after_key(mut self, key: String) -> Self {
        self.start_after_key = StartAfter::Key(key);
        self
    }

    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = filter;
        self
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }
}

impl From<Filter> for ListProps {
    fn from(filter: Filter) -> Self {
        Self {
            start_after_key: StartAfter::None,
            filter,
            order: Order::Asc,
            limit: 10,
        }
    }
}

impl From<Order> for ListProps {
    fn from(order: Order) -> Self {
        Self {
            start_after_key: StartAfter::None,
            filter: Filter::None,
            order,
            limit: 10,
        }
    }
}

impl From<StartAfter> for ListProps {
    fn from(start_after_key: StartAfter) -> Self {
        Self {
            start_after_key,
            filter: Filter::None,
            order: Order::Asc,
            limit: 10,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Cache<V>
where
    V: PartialEq,
{
    map: HashMap<String, V>,
    list: Vec<String>,
    capacity: usize,
    _phantom: std::marker::PhantomData<V>,
}

impl<V> Cache<V>
where
    V: PartialEq,
{
     pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            list: Vec::new(),
            capacity,
            _phantom: std::marker::PhantomData,
        }
    }

    pub fn insert(&mut self, key: &str, value: V) {
        let key = key.to_string();
        if let Some(value) = self.map.get(&key) {
            if value.eq(value) {
                return;
            }
        }

        if self.map.len() != 0 && self.map.len() == self.capacity {
            let first_key = self.list.remove(0);
            self.map.remove(&first_key);
        }

        // sorted insert
        let position = self
            .list
            .iter()
            .position(|k| k > &key)
            .unwrap_or(self.list.len());
        self.list.insert(position, key.to_string());
        self.map.insert(key.to_string(), value.into());
    }

    pub fn insert_if_not_exists(&mut self, key: &str, value: V) -> Result<(), Error> {
        if self.map.contains_key(key) {
            return Err(Error::SortKeyExists);
        }

        self.insert(key, value);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Option<&V> {
        self.map.get(key)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut V> {
        self.map.get_mut(key)
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn remove(&mut self, key: &str) {
        let position = self.list.iter().position(|k| k == &key).unwrap();

        self.list.remove(position);
        self.map.remove(key);
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.list.clear();
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub fn list<T>(&self, props: T) -> Result<Vec<(String, &V)>, Error>
    where
        T: Into<ListProps>,
    {
        let props = props.into();

        let position = match props.start_after_key {
            StartAfter::Key(key) => {
                self.list
                    .iter()
                    .position(|k| k == &key)
                    .ok_or(Error::SortKeyNotFound)?
                    + 1
            }
            StartAfter::None => 0,
        };

        let mut list = Vec::new();
        let mut count = 0;

        match props.order {
            Order::Asc => {
                let skip_iter = self.list.iter().skip(position);
                for k in skip_iter {
                    let filtered: Option<(String, &V)> = match props.filter.clone() {
                        Filter::StartWith(key) => {
                            if k.starts_with(&key.to_string()) {
                                Some((k.clone(), self.map.get(k).unwrap()))
                            } else {
                                None
                            }
                        }
                        Filter::EndWith(key) => {
                            if k.ends_with(&key.to_string()) {
                                Some((k.clone(), self.map.get(k).unwrap()))
                            } else {
                                None
                            }
                        }
                        Filter::StartAndEndWith(start_key, end_key) => {
                            if k.starts_with(&start_key.to_string())
                                && k.ends_with(&end_key.to_string())
                            {
                                Some((k.clone(), self.map.get(k).unwrap()))
                            } else {
                                None
                            }
                        }
                        Filter::None => Some((k.clone(), self.map.get(k).unwrap())),
                    };

                    if let Some(item) = filtered {
                        list.push(item);
                        count += 1;
                        if count == props.limit {
                            break;
                        }
                    }
                }
            }
            Order::Desc => {
                let skip_iter = self.list.iter().rev().skip(position);
                for k in skip_iter {
                    let filtered: Option<(String, &V)> = match props.filter.clone() {
                        Filter::StartWith(key) => {
                            if k.starts_with(&key.to_string()) {
                                Some((k.clone(), self.map.get(k).unwrap()))
                            } else {
                                None
                            }
                        }
                        Filter::EndWith(key) => {
                            if k.ends_with(&key.to_string()) {
                                Some((k.clone(), self.map.get(k).unwrap()))
                            } else {
                                None
                            }
                        }
                        Filter::StartAndEndWith(start_key, end_key) => {
                            if k.starts_with(&start_key.to_string())
                                && k.ends_with(&end_key.to_string())
                            {
                                Some((k.clone(), self.map.get(k).unwrap()))
                            } else {
                                None
                            }
                        }
                        Filter::None => Some((k.clone(), self.map.get(k).unwrap())),
                    };

                    if let Some(item) = filtered {
                        list.push(item);
                        count += 1;
                        if count == props.limit {
                            break;
                        }
                    }
                }
            }
        };

        Ok(list)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_cache_insert() {
        let mut cache = Cache::new(2);
        cache.insert("key1", Value::from(1));
        cache.insert("key2", Value::from(2));
        cache.insert("key3", Value::from(3));
        assert_eq!(cache.get("key1"), None);
        assert_eq!(cache.get("key2"), Some(&Value::from(2)));
        assert_eq!(cache.get("key3"), Some(&Value::from(3)));
    }

    #[test]
    fn test_cache_remove() {
        let mut cache = Cache::new(2);
        cache.insert("key1", Value::from(1));
        cache.insert("key2", Value::from(2));
        cache.remove("key1");
        assert_eq!(cache.get("key1"), None);
        cache.insert("key3", Value::from(3));
        assert_eq!(cache.get("key3"), Some(&Value::from(3)));
        assert_eq!(cache.get("key2"), Some(&Value::from(2)));
    }

    #[test]
    fn test_cache_clear() {
        let mut cache = Cache::<Value>::new(2);
        cache.insert("key1", Value::from(1));
        cache.insert("key2", Value::from(2));
        cache.clear();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_list_asc() {
        let mut cache = Cache::new(5);
        cache.insert("key2", Value::from(2));
        cache.insert("key1", Value::from(1));
        cache.insert("key5", Value::from(5));
        cache.insert("key4", Value::from(4));
        cache.insert("key3", Value::from(3));

        let result_res = cache.list(StartAfter::Key("key2".to_string()));

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("key3".to_string(), &Value::from(3)));
        assert_eq!(result[1], ("key4".to_string(), &Value::from(4)));
        assert_eq!(result[2], ("key5".to_string(), &Value::from(5)));
    }

    #[test]
    fn test_cache_list_desc() {
        let mut cache = Cache::new(5);
        cache.insert("key5", Value::from(5));
        cache.insert("key1", Value::from(1));
        cache.insert("key3", Value::from(3));
        cache.insert("key4", Value::from(4));
        cache.insert("key2", Value::from(2));

        let result_res = cache.list(ListProps {
            order: Order::Desc,
            filter: Filter::None,
            start_after_key: StartAfter::Key("key3".to_string()),
            limit: 10,
        });

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("key2".to_string(), &Value::from(2)));
        assert_eq!(result[1], ("key1".to_string(), &Value::from(1)));
    }

    #[test]
    fn test_filter_start_with() {
        let mut cache = Cache::new(10);

        cache.insert("postmodern", Value::from(8));
        cache.insert("postpone", Value::from(6));
        cache.insert("precept", Value::from(2));
        cache.insert("postmortem", Value::from(9));
        cache.insert("precaution", Value::from(3));
        cache.insert("precede", Value::from(1));
        cache.insert("precognition", Value::from(5));
        cache.insert("postmark", Value::from(10));
        cache.insert("postgraduate", Value::from(7));
        cache.insert("preconceive", Value::from(4));

        let result_res = cache.list(Filter::StartWith("postm".to_string()));

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 3);
        assert_eq!(result[0], ("postmark".to_string(), &Value::from(10)));
        assert_eq!(result[1], ("postmodern".to_string(), &Value::from(8)));
        assert_eq!(result[2], ("postmortem".to_string(), &Value::from(9)));
    }

    #[test]
    fn test_filter_ends_with() {
        let mut cache = Cache::new(10);

        cache.insert("postmodern", Value::from(8));
        cache.insert("postpone", Value::from(6));
        cache.insert("precept", Value::from(2));
        cache.insert("postmortem", Value::from(9));
        cache.insert("precaution", Value::from(3));
        cache.insert("precede", Value::from(1));
        cache.insert("precognition", Value::from(5));
        cache.insert("postmark", Value::from(10));
        cache.insert("postgraduate", Value::from(7));
        cache.insert("preconceive", Value::from(4));

        let result_res = cache.list(Filter::EndWith("tion".to_string()));

        assert_eq!(result_res.is_ok(), true);

        let result = match result_res {
            Ok(result) => result,
            Err(_) => panic!("Error"),
        };

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("precaution".to_string(), &Value::from(3)));
        assert_eq!(result[1], ("precognition".to_string(), &Value::from(5)));
    }
}
