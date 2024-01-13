use std::env;

pub struct Database {
    pub capacity: usize,
    pub table_capacity: usize,
    pub partition_capacity: usize,
}