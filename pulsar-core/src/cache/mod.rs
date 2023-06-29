mod cache;
use std::sync::{Arc, Mutex, MutexGuard};

use cache::Cache;
use valu3::value::Value;

#[derive(Clone, Debug)]
pub struct Table {
    cache: Arc<Mutex<Cache<Value>>>,
}

impl Table {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(Cache::new(capacity))),
        }
    }

    pub fn get_cache(&self) -> MutexGuard<'_, Cache<Value>> {
        match self.cache.lock() {
            Ok(cache) => cache,
            Err(_) => panic!("Could not lock cache"),
        }
    }
}

impl PartialEq for Table {
    fn eq(&self, other: &Self) -> bool {
        *self.cache.lock().unwrap() == *other.cache.lock().unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct Controller {
    cache: Arc<Mutex<Cache<Table>>>,
}

impl Controller {
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(Cache::new(capacity))),
        }
    }

    pub fn get_cache(&self) -> MutexGuard<'_, Cache<Table>>  {
        match self.cache.lock() {
            Ok(cache) => cache,
            Err(_) => panic!("Could not lock cache"),
        }
    }

    pub fn create_table(&self, table_name: &str, capacity: usize) {
        self.get_cache().insert(table_name, Table::new(capacity))
    }

    pub fn get_table_capacity(&self, table_name: &str) -> Option<usize> {
        match self.get_cache().get(table_name) {
            Some(table) => Some(table.get_cache().capacity()),
            None => None,
        }
    }

    pub fn get_table_len(&self, table_name: &str) -> Option<usize> {
        match self.get_cache().get(table_name) {
            Some(table) => Some(table.get_cache().len()),
            None => None,
        }
    }

    pub fn get_table_mut(&self, table_name: &str) -> MutexGuard<'_, Cache<Table>> {
        match self.cache.lock() {
            Ok(cache) => cache,
            Err(_) => panic!("Could not lock cache"),
        }
    }

    pub fn get_table(&self, table_name: &str) -> Option<Table> {
        match self.get_cache().get(table_name) {
            Some(table) => Some(table.clone()),
            None => None,
        }
    }

    pub fn remove_table(&self, table_name: &str) {
        self.get_cache().remove(table_name);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table() {
        let controller = Controller::new(10);
        controller.create_table("test", 10);
        assert_eq!(controller.get_table_capacity("test"), Some(10));
    }

    #[test]
    fn test_get_table_len() {
        let controller = Controller::new(10);
        controller.create_table("test", 10);
        assert_eq!(controller.get_table_len("test"), Some(0));
    }

    #[test]
    fn test_get_table_mut() {
        let controller = Controller::new(10);
        controller.create_table("test", 10);
        assert_eq!(controller.get_table_mut("test").capacity(), 10);
    }

    #[test]
    fn test_get_table() {
        let controller = Controller::new(10);
        controller.create_table("test", 10);
        assert_eq!(controller.get_table("test").unwrap().get_cache().capacity(), 10);
    }

    #[test]
    fn test_remove_table() {
        let controller = Controller::new(10);
        controller.create_table("test", 10);
        controller.remove_table("test");
        assert_eq!(controller.get_table("test"), None);
    }

    #[test]
    fn test_table_eq() {
        let controller = Controller::new(10);
        controller.create_table("test", 10);
        let table = controller.get_table("test").unwrap();
        assert_eq!(controller.get_table("test").unwrap(), table);
    }
}
