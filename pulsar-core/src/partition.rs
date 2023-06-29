use std::{collections::HashMap, fmt::Display};
use valu3::prelude::*;
use std::fmt::Debug;

pub enum Error {
    SortKeyNotFound,
    PartitionAlreadyExists,
    SortKeyExists,
    TableAlreadyExists
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SortKeyNotFound => write!(f, "Sort key not found"),
            Error::PartitionAlreadyExists => write!(f, "Partition already exists"),
            Error::SortKeyExists => write!(f, "Sort key exists"),
            Error::TableAlreadyExists => write!(f, "Table already exists"),
        }
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

#[derive(Debug)]
pub enum Filter<'a> {
    StartWith(&'a str),
    EndWith(&'a str),
    StartAndEndWith(&'a str, &'a str),
    None,
}

impl Default for Filter<'_> {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug)]
pub enum Order {
    Asc,
    Desc,
}

impl Default for Order {
    fn default() -> Self {
        Self::Asc
    }
}

#[derive(Debug)]
pub enum StartAfter<'a> {
    Key(&'a str),
    None,
}

impl Default for StartAfter<'_> {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Default, Debug)]
pub struct ListProps<'a> {
    pub start_after_key: StartAfter<'a>,
    pub filter: Filter<'a>,
    pub order: Order,
    pub limit: usize,
}

impl<'a> ListProps<'a> {
    fn new() -> Self {
        Self {
            start_after_key: StartAfter::None,
            filter: Filter::None,
            order: Order::Asc,
            limit: 10,
        }
    }

    pub fn start_after_key(mut self, key: &'a str) -> Self {
        self.start_after_key = StartAfter::Key(key);
        self
    }

    pub fn filter(mut self, filter: Filter<'a>) -> Self {
        self.filter = filter;
        self
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }
}

impl<'a> From<Filter<'a>> for ListProps<'a> {
    fn from(filter: Filter<'a>) -> Self {
        Self {
            start_after_key: StartAfter::None,
            filter,
            order: Order::Asc,
            limit: 10,
        }
    }
}

impl<'a> From<Order> for ListProps<'a> {
    fn from(order: Order) -> Self {
        Self {
            start_after_key: StartAfter::None,
            filter: Filter::None,
            order,
            limit: 10,
        }
    }
}

impl<'a> From<StartAfter<'a>> for ListProps<'a> {
    fn from(start_after_key: StartAfter<'a>) -> Self {
        Self {
            start_after_key,
            filter: Filter::None,
            order: Order::Asc,
            limit: 10,
        }
    }
}

#[macro_export]
macro_rules! filter_and_push {
    ($filter:expr, $map:expr, $list:expr, $count:expr, $limit:expr, $filter_fn:expr, $skip_iter:expr) => {
        for k in $skip_iter {
            let filtered = match $filter {
                Filter::StartWith(key) => {
                    if k.starts_with(key) {
                        Some((*k, $map.get(k).unwrap()))
                    } else {
                        None
                    }
                }
                Filter::EndWith(key) => {
                    if k.ends_with(key) {
                        Some((*k, $map.get(k).unwrap()))
                    } else {
                        None
                    }
                }
                Filter::StartAndEndWith(start_key, end_key) => {
                    if k.starts_with(start_key) && k.ends_with(end_key) {
                        Some((*k, $map.get(k).unwrap()))
                    } else {
                        None
                    }
                }
                Filter::None => Some((*k, $map.get(k).unwrap())),
            };

            if let Some(item) = filtered {
                $list.push(item);
                $count += 1;
                if $count == $limit {
                    break;
                }
            }
        }
    };
}

pub trait PartitionTrait<'a> {
    type Output;

    fn insert<V>(&mut self, key: &'a str, value: V)
    where
        V: Into<Self::Output>;
    fn insert_if_not_exists<V>(&mut self, key: &'a str, value: V) -> Result<(), Error>
    where
        V: Into<Self::Output>;
    fn get(&self, key: &str) -> Option<&Self::Output>;
    fn get_mut(&mut self, key: &str) -> Option<&mut Self::Output>;
    fn remove(&mut self, key: &str);
    fn clear(&mut self);
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn contains_key(&self, key: &str) -> bool;
    fn list<T>(&self, props: T) -> Result<Vec<(&str, &Self::Output)>, Error>
    where
        T: Into<ListProps<'a>>;
}

#[derive(Clone, Debug, PartialEq)]
pub struct Partition<'a> {
    map: HashMap<&'a str, Value>,
    list: Vec<&'a str>,
    capacity: usize,
}

impl<'a> Partition<'a> {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            list: Vec::new(),
            capacity,
        }
    }
}

impl<'a> PartitionTrait<'a> for Partition<'a> {
    type Output = Value;

    fn insert<V>(&mut self, key: &'a str, value: V)
    where
        V: Into<Value>,
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
        V: Into<Value>,
    {
        if self.map.contains_key(key) {
            return Err(Error::SortKeyExists);
        }

        self.insert(key, value);
        Ok(())
    }

    fn get(&self, key: &str) -> Option<&Value> {
        self.map.get(key)
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.map.get_mut(key)
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

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    fn list<T>(&self, props: T) -> Result<Vec<(&str, &Value)>, Error>
    where
        T: Into<ListProps<'a>>,
    {
        let props = props.into();

        let position = match props.start_after_key {
            StartAfter::Key(key) => {
                self.list
                    .iter()
                    .position(|&k| k == key)
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
        partition.insert("key2", Value::from(2));
        partition.insert("key1", Value::from(1));
        partition.insert("key5", Value::from(5));
        partition.insert("key4", Value::from(4));
        partition.insert("key3", Value::from(3));

        let result_res = partition.list(StartAfter::Key("key2"));

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
        partition.insert("key5", Value::from(5));
        partition.insert("key1", Value::from(1));
        partition.insert("key3", Value::from(3));
        partition.insert("key4", Value::from(4));
        partition.insert("key2", Value::from(2));

        let result_res = partition.list(ListProps {
            order: Order::Desc,
            filter: Filter::None,
            start_after_key: StartAfter::Key("key3"),
            limit: 10,
        });

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

        let result_res = partition.list(Filter::StartWith("postm"));

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

        let result_res = partition.list(Filter::EndWith("tion"));

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
