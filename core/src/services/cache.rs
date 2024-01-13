use std::sync::{Arc, Mutex};

use crate::{
    cache::{
        cache::{Cache, Error, ListProps},
        table::Table, partition::Partition,
    },
    events::Events,
};

pub struct CacheService {
    pub tables: Cache<Table>,
    pub events: Arc<Mutex<Events>>,
}

impl CacheService {
    pub fn build(capacity: usize, events: Arc<Mutex<Events>>) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self::new(capacity, events)))
    }

    pub fn new(capacity: usize, events: Arc<Mutex<Events>>) -> Self {
        Self {
            tables: Cache::new(capacity),
            events,
        }
    }

    pub fn create_table(&mut self, table_name: &'static str, capacity: usize) {
        self.tables.insert(table_name, Table::new(capacity));
    }

    pub fn create_table_if_not_exists(&mut self, table_name: &'static str, capacity: usize) {
        self.tables
            .insert_if_not_exists(table_name, Table::new(capacity));
    }

    pub fn remove_table(&mut self, table_name: &'static str) {
        self.tables.remove(table_name);
    }

    pub fn table_exists(&self, table_name: &'static str) -> bool {
        self.tables.contains_key(table_name)
    }

    pub fn update_tables(&mut self, capacity: usize) {
        self.tables.set_capacity(capacity)
    }

    pub fn clear_tables(&mut self) {
        self.tables.clear();
    }

    pub fn list_table(
        &self,
        table_name: &'static str,
        props: ListProps,
    ) -> Result<Vec<(&str, &Partition)>, Error> {
        let table: &Cache<Partition> = self.tables.get(table_name).unwrap();
        table.list(props)
    }

    pub fn create_partition(
        &mut self,
        table_name: &str,
        partition_key: &'static str,
        value: Partition,
    ) {
        let table: &mut Cache<Partition> = self.tables.get_mut(table_name).unwrap();
        table.insert(partition_key, value);
    }

    pub fn create_partition_if_not_exists(
        &mut self,
        table_name: &str,
        partition_key: &'static str,
        value: Partition,
    ) {
        let table = self.tables.get_mut(table_name).unwrap();
        table.insert_if_not_exists(partition_key, value);
    }

    pub fn get_partition(&self, table_name: &str, partition_key: &'static str) -> Option<&Partition> {
        let table = self.tables.get(table_name).unwrap();
        table.get(partition_key)
    }

    pub fn update_partition(
        &mut self,
        table_name: &str,
        partition_key: &'static str,
        value: Partition,
    ) {
        let table = self.tables.get_mut(table_name).unwrap();
        table.insert(partition_key, value);
    }

    pub fn remove_partition(&mut self, table_name: &str, partition_key: &'static str) {
        let table: &mut Cache<Partition> = self.tables.get_mut(table_name).unwrap();
        table.remove(partition_key);
    }

    pub fn partition_exists(&self, table_name: &str, partition_key: &'static str) -> bool {
        let table: &Cache<Partition> = self.tables.get(table_name).unwrap();
        table.contains_key(partition_key)
    }
}

