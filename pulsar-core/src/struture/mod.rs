mod partition;
mod table;
mod database;
use std::{fmt::Display};
use std::fmt::Debug;
pub use {
    database::Database,
    partition::Partition,
    table::Table,
};
pub use valu3::prelude::*;

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
